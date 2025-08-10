"""
StateManagementService: Handles strategy state persistence and recovery.

Provides REQ/REP interface for saving and loading strategy states.
Uses DuckDB for durable storage with JSON serialization.
"""

import asyncio
import json
from typing import Dict, Any, Optional
import duckdb

from core import AbstractService, MessageBus, Ports


class StateManagementService(AbstractService):
    """
    Service for managing strategy state persistence.
    
    Provides:
    1. save_state(strategy_id, state_dict) - Save strategy state
    2. load_state(strategy_id) - Load strategy state
    3. Fault recovery through durable DuckDB storage
    4. State versioning for rollback capabilities
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__("state_management", config)
        self.message_bus = StateManagementMessageBus("state_management")
        
        # Configuration
        self.db_path = config.get('duckdb_path', 'data/states.db')
        self.max_state_versions = config.get('max_state_versions_per_strategy', 10)
        
        # Database connection
        self.db_conn: Optional[duckdb.DuckDBPyConnection] = None
    
    async def initialize(self):
        """Initialize state management service and database."""
        await super().initialize()
        await self.message_bus.initialize()
        await self._initialize_database()
        
        # Set service reference for message handling
        self.message_bus.set_state_service(self)
    
    async def _initialize_database(self):
        """Initialize DuckDB for state storage."""
        try:
            self.db_conn = duckdb.connect(self.db_path)
            
            # Create strategy_states table
            self.db_conn.execute("""
                CREATE TABLE IF NOT EXISTS strategy_states (
                    id INTEGER PRIMARY KEY,
                    strategy_id VARCHAR UNIQUE,
                    state_json TEXT,
                    version INTEGER DEFAULT 1,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create state_history table for versioning
            self.db_conn.execute("""
                CREATE TABLE IF NOT EXISTS state_history (
                    id INTEGER PRIMARY KEY,
                    strategy_id VARCHAR,
                    state_json TEXT,
                    version INTEGER,
                    saved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            self.db_conn.commit()
            self.logger.info(f"Initialized state database: {self.db_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize state database: {e}")
            raise
    
    async def async_run(self):
        """Main state management service loop."""
        await self.initialize()
        self._running = True
        
        try:
            # Start REQ/REP response loop
            await self.message_bus.response_loop(Ports.STATE_MANAGEMENT)
            
        except Exception as e:
            self.logger.error(f"StateManagement service error: {e}")
            raise
        finally:
            await self.cleanup()
    
    async def save_state(self, strategy_id: str, state_dict: Dict[str, Any]) -> bool:
        """
        Save strategy state to database.
        
        Args:
            strategy_id: Unique identifier for the strategy
            state_dict: Strategy state to be persisted
            
        Returns:
            True if successful, False otherwise
        """
        try:
            state_json = json.dumps(state_dict)
            current_time = asyncio.get_event_loop().time()
            
            # Check if strategy already has a state
            existing = self.db_conn.execute(
                "SELECT version FROM strategy_states WHERE strategy_id = ?",
                (strategy_id,)
            ).fetchone()
            
            if existing:
                # Update existing state
                new_version = existing[0] + 1
                
                # Archive current state to history
                current_state = self.db_conn.execute(
                    "SELECT state_json FROM strategy_states WHERE strategy_id = ?",
                    (strategy_id,)
                ).fetchone()
                
                if current_state:
                    self.db_conn.execute("""
                        INSERT INTO state_history (strategy_id, state_json, version)
                        VALUES (?, ?, ?)
                    """, (strategy_id, current_state[0], existing[0]))
                
                # Update main state
                self.db_conn.execute("""
                    UPDATE strategy_states 
                    SET state_json = ?, version = ?, last_updated = CURRENT_TIMESTAMP
                    WHERE strategy_id = ?
                """, (state_json, new_version, strategy_id))
                
            else:
                # Create new state record
                self.db_conn.execute("""
                    INSERT INTO strategy_states (strategy_id, state_json)
                    VALUES (?, ?)
                """, (strategy_id, state_json))
            
            # Clean up old history versions if necessary
            await self._cleanup_old_versions(strategy_id)
            
            self.db_conn.commit()
            self.logger.debug(f"Saved state for strategy {strategy_id}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to save state for {strategy_id}: {e}")
            return False
    
    async def load_state(self, strategy_id: str) -> Optional[Dict[str, Any]]:
        """
        Load strategy state from database.
        
        Args:
            strategy_id: Unique identifier for the strategy
            
        Returns:
            Strategy state dictionary or None if not found
        """
        try:
            result = self.db_conn.execute(
                "SELECT state_json FROM strategy_states WHERE strategy_id = ?",
                (strategy_id,)
            ).fetchone()
            
            if result:
                state_dict = json.loads(result[0])
                self.logger.debug(f"Loaded state for strategy {strategy_id}")
                return state_dict
            else:
                self.logger.info(f"No saved state found for strategy {strategy_id}")
                return None
                
        except Exception as e:
            self.logger.error(f"Failed to load state for {strategy_id}: {e}")
            return None
    
    async def load_state_version(self, strategy_id: str, version: int) -> Optional[Dict[str, Any]]:
        """
        Load a specific version of strategy state.
        
        Args:
            strategy_id: Unique identifier for the strategy
            version: State version number to load
            
        Returns:
            Strategy state dictionary or None if not found
        """
        try:
            # First check if it's the current version
            current = self.db_conn.execute(
                "SELECT state_json, version FROM strategy_states WHERE strategy_id = ?",
                (strategy_id,)
            ).fetchone()
            
            if current and current[1] == version:
                return json.loads(current[0])
            
            # Check history for the requested version
            historical = self.db_conn.execute(
                "SELECT state_json FROM state_history WHERE strategy_id = ? AND version = ?",
                (strategy_id, version)
            ).fetchone()
            
            if historical:
                state_dict = json.loads(historical[0])
                self.logger.debug(f"Loaded version {version} state for strategy {strategy_id}")
                return state_dict
            else:
                self.logger.warning(f"Version {version} not found for strategy {strategy_id}")
                return None
                
        except Exception as e:
            self.logger.error(f"Failed to load state version {version} for {strategy_id}: {e}")
            return None
    
    async def list_strategy_states(self) -> Dict[str, Dict[str, Any]]:
        """
        List all strategy states with metadata.
        
        Returns:
            Dictionary mapping strategy_id to state metadata
        """
        try:
            results = self.db_conn.execute("""
                SELECT strategy_id, version, last_updated
                FROM strategy_states
                ORDER BY last_updated DESC
            """).fetchall()
            
            states_info = {}
            for row in results:
                states_info[row[0]] = {
                    'strategy_id': row[0],
                    'current_version': row[1],
                    'last_updated': row[2]
                }
            
            self.logger.debug(f"Listed {len(states_info)} strategy states")
            return states_info
            
        except Exception as e:
            self.logger.error(f"Failed to list strategy states: {e}")
            return {}
    
    async def _cleanup_old_versions(self, strategy_id: str):
        """Clean up old state versions to maintain storage limits."""
        try:
            # Count current versions
            version_count = self.db_conn.execute(
                "SELECT COUNT(*) FROM state_history WHERE strategy_id = ?",
                (strategy_id,)
            ).fetchone()[0]
            
            if version_count > self.max_state_versions:
                # Delete oldest versions
                versions_to_delete = version_count - self.max_state_versions
                self.db_conn.execute("""
                    DELETE FROM state_history 
                    WHERE strategy_id = ? 
                    ORDER BY saved_at ASC 
                    LIMIT ?
                """, (strategy_id, versions_to_delete))
                
                self.logger.debug(f"Cleaned up {versions_to_delete} old versions for {strategy_id}")
                
        except Exception as e:
            self.logger.error(f"Failed to cleanup old versions for {strategy_id}: {e}")
    
    async def cleanup(self):
        """Clean up state management service resources."""
        await super().cleanup()
        await self.message_bus.cleanup()
        
        if self.db_conn:
            self.db_conn.close()
            self.logger.info("Closed state database connection")
        
        self.logger.info("StateManagement service cleaned up")


class StateManagementMessageBus(MessageBus):
    """
    Custom MessageBus for StateManagementService.
    
    Handles REQ/REP pattern for state operations.
    """
    
    def __init__(self, service_name: str):
        super().__init__(service_name)
        self.state_service: Optional[StateManagementService] = None
    
    def set_state_service(self, service: StateManagementService):
        """Set reference to state management service."""
        self.state_service = service
    
    async def _handle_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle state management requests.
        
        Supported operations:
        - save_state: Save strategy state
        - load_state: Load strategy state
        - load_state_version: Load specific state version
        - list_states: List all strategy states
        """
        if not self.state_service:
            return {'error': 'State service not initialized'}
        
        try:
            operation = request.get('data', {}).get('operation')
            params = request.get('data', {}).get('params', {})
            
            if operation == 'save_state':
                strategy_id = params.get('strategy_id')
                state_dict = params.get('state_dict', {})
                
                if not strategy_id:
                    return {'error': 'strategy_id is required for save_state'}
                
                success = await self.state_service.save_state(strategy_id, state_dict)
                return {'success': success}
            
            elif operation == 'load_state':
                strategy_id = params.get('strategy_id')
                
                if not strategy_id:
                    return {'error': 'strategy_id is required for load_state'}
                
                state_dict = await self.state_service.load_state(strategy_id)
                return {'state': state_dict}
            
            elif operation == 'load_state_version':
                strategy_id = params.get('strategy_id')
                version = params.get('version')
                
                if not strategy_id or version is None:
                    return {'error': 'strategy_id and version are required for load_state_version'}
                
                state_dict = await self.state_service.load_state_version(strategy_id, version)
                return {'state': state_dict}
            
            elif operation == 'list_states':
                states_info = await self.state_service.list_strategy_states()
                return {'states': states_info}
            
            else:
                return {'error': f'Unknown operation: {operation}'}
        
        except Exception as e:
            self.logger.error(f"Request handling error: {e}")
            return {'error': str(e)}