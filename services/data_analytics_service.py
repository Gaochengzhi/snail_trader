"""
DataAnalyticsService: Collects task results and triggers strategy reflection.

Receives task results via PUSH/PULL pattern and stores them in DuckDB.
When analysis conditions are met, performs analytics and publishes reflection updates.
"""

import asyncio
import json
from typing import Dict, Any, Optional
import duckdb

from core import AbstractService, MessageBus, Topics, Ports


class DataAnalyticsService(AbstractService):
    """
    Service for collecting and analyzing task results.
    
    Responsibilities:
    1. Collect task results from all strategies via PUSH/PULL
    2. Store results in DuckDB for persistence and analysis
    3. Perform periodic analysis when conditions are met
    4. Generate reflection events to update strategy prompts
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__("data_analytics", config)
        self.message_bus = DataAnalyticsMessageBus("data_analytics")
        
        # Configuration
        self.db_path = config.get('duckdb_path', 'data/analytics.db')
        self.analysis_trigger_threshold = config.get('analysis_trigger_threshold', 100)
        self.analysis_interval_minutes = config.get('analysis_interval_minutes', 30)
        
        # Analytics state
        self.db_conn: Optional[duckdb.DuckDBPyConnection] = None
        self.task_results_count = 0
        self.last_analysis_time = 0
    
    async def initialize(self):
        """Initialize analytics service and database."""
        await super().initialize()
        await self.message_bus.initialize()
        await self._initialize_database()
    
    async def _initialize_database(self):
        """Initialize DuckDB for storing task results and analytics."""
        try:
            self.db_conn = duckdb.connect(self.db_path)
            
            # Create task_results table
            self.db_conn.execute("""
                CREATE TABLE IF NOT EXISTS task_results (
                    id INTEGER PRIMARY KEY,
                    task_id VARCHAR,
                    strategy_id VARCHAR,
                    timestamp DOUBLE,
                    result_type VARCHAR,
                    result_data TEXT,
                    error_message VARCHAR,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create analytics_reports table for storing analysis results
            self.db_conn.execute("""
                CREATE TABLE IF NOT EXISTS analytics_reports (
                    id INTEGER PRIMARY KEY,
                    report_type VARCHAR,
                    strategy_id VARCHAR,
                    analysis_period_start DOUBLE,
                    analysis_period_end DOUBLE,
                    metrics TEXT,
                    recommendations TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            self.db_conn.commit()
            self.logger.info(f"Initialized analytics database: {self.db_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize database: {e}")
            raise
    
    async def async_run(self):
        """Main analytics service loop."""
        await self.initialize()
        self._running = True
        
        try:
            # Start task results collection loop
            results_task = asyncio.create_task(
                self.message_bus.pull_results_loop(Ports.TASK_RESULTS)
            )
            
            # Start periodic analysis loop
            analysis_task = asyncio.create_task(self._analysis_loop())
            
            # Wait for both tasks
            await asyncio.gather(results_task, analysis_task)
            
        except Exception as e:
            self.logger.error(f"DataAnalytics service error: {e}")
            raise
        finally:
            await self.cleanup()
    
    async def _analysis_loop(self):
        """
        Periodic analysis loop.
        
        Triggers analysis based on:
        1. Task results count threshold
        2. Time interval threshold
        """
        while self._running:
            try:
                current_time = asyncio.get_event_loop().time()
                time_since_last_analysis = current_time - self.last_analysis_time
                
                # Check if we should trigger analysis
                should_analyze = (
                    self.task_results_count >= self.analysis_trigger_threshold or
                    time_since_last_analysis >= (self.analysis_interval_minutes * 60)
                )
                
                if should_analyze:
                    await self._perform_analysis()
                    self.last_analysis_time = current_time
                    self.task_results_count = 0  # Reset counter
                
                # Check every minute
                await asyncio.sleep(60)
                
            except Exception as e:
                self.logger.error(f"Analysis loop error: {e}")
                await asyncio.sleep(60)
    
    async def store_task_result(self, result_data: Dict[str, Any]):
        """
        Store task result in database.
        
        Called by the message bus when task results are received.
        """
        try:
            # Extract result information
            task_id = result_data.get('task_id', 'unknown')
            strategy_id = result_data.get('strategy_id', 'unknown')
            timestamp = result_data.get('timestamp', 0)
            
            # Determine if this is a result or error
            if 'error' in result_data:
                result_type = 'error'
                result_json = json.dumps({'error': result_data['error']})
                error_message = result_data['error']
            else:
                result_type = 'success'
                result_json = json.dumps(result_data.get('result', {}))
                error_message = None
            
            # Insert into database
            self.db_conn.execute("""
                INSERT INTO task_results 
                (task_id, strategy_id, timestamp, result_type, result_data, error_message)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (task_id, strategy_id, timestamp, result_type, result_json, error_message))
            
            self.db_conn.commit()
            self.task_results_count += 1
            
            self.logger.debug(f"Stored task result: {task_id} from {strategy_id}")
            
        except Exception as e:
            self.logger.error(f"Failed to store task result: {e}")
    
    async def _perform_analysis(self):
        """
        Perform analysis on accumulated task results.
        
        This method analyzes the task_results table and generates
        insights that can be used to update strategy behavior.
        """
        try:
            self.logger.info("Starting analysis of task results")
            
            # TODO: Implement actual analysis logic
            # This could include:
            # 1. Success/failure rate analysis by strategy
            # 2. Performance trend analysis
            # 3. Error pattern detection
            # 4. Strategy comparison metrics
            # 5. Risk assessment
            
            # Example analysis queries:
            analyses = await self._run_analysis_queries()
            
            # Generate recommendations based on analysis
            recommendations = await self._generate_recommendations(analyses)
            
            # Store analysis report
            await self._store_analysis_report(analyses, recommendations)
            
            # Publish reflection updates if needed
            if recommendations:
                await self._publish_reflection_updates(recommendations)
            
            self.logger.info("Analysis completed successfully")
            
        except Exception as e:
            self.logger.error(f"Analysis failed: {e}")
    
    async def _run_analysis_queries(self) -> Dict[str, Any]:
        """
        Run analysis queries on task results.
        
        Returns:
            Dictionary containing analysis results
        """
        analyses = {}
        
        try:
            # Strategy performance analysis
            strategy_stats = self.db_conn.execute("""
                SELECT 
                    strategy_id,
                    COUNT(*) as total_tasks,
                    SUM(CASE WHEN result_type = 'success' THEN 1 ELSE 0 END) as successful_tasks,
                    SUM(CASE WHEN result_type = 'error' THEN 1 ELSE 0 END) as failed_tasks,
                    AVG(timestamp) as avg_timestamp
                FROM task_results 
                WHERE timestamp > ?
                GROUP BY strategy_id
            """, (asyncio.get_event_loop().time() - 3600,)).fetchall()  # Last hour
            
            analyses['strategy_performance'] = [
                {
                    'strategy_id': row[0],
                    'total_tasks': row[1],
                    'successful_tasks': row[2],
                    'failed_tasks': row[3],
                    'success_rate': row[2] / row[1] if row[1] > 0 else 0,
                    'avg_timestamp': row[4]
                }
                for row in strategy_stats
            ]
            
            # Error pattern analysis
            error_patterns = self.db_conn.execute("""
                SELECT 
                    error_message,
                    COUNT(*) as error_count,
                    strategy_id
                FROM task_results 
                WHERE result_type = 'error' 
                    AND timestamp > ?
                GROUP BY error_message, strategy_id
                ORDER BY error_count DESC
                LIMIT 10
            """, (asyncio.get_event_loop().time() - 3600,)).fetchall()
            
            analyses['error_patterns'] = [
                {
                    'error_message': row[0],
                    'count': row[1],
                    'strategy_id': row[2]
                }
                for row in error_patterns
            ]
            
            self.logger.debug(f"Analysis completed: {len(analyses)} analysis types")
            
        except Exception as e:
            self.logger.error(f"Analysis queries failed: {e}")
            analyses = {}
        
        return analyses
    
    async def _generate_recommendations(self, analyses: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate recommendations based on analysis results.
        
        Args:
            analyses: Results from _run_analysis_queries
            
        Returns:
            Dictionary containing recommendations for strategy updates
        """
        recommendations = {}
        
        try:
            # TODO: Implement recommendation logic
            # This could include:
            # 1. Suggest parameter adjustments for underperforming strategies
            # 2. Recommend strategy disabling if error rates are too high
            # 3. Suggest increasing task frequency for successful strategies
            # 4. Recommend risk adjustments based on market conditions
            
            strategy_recommendations = {}
            
            for strategy_perf in analyses.get('strategy_performance', []):
                strategy_id = strategy_perf['strategy_id']
                success_rate = strategy_perf['success_rate']
                
                if success_rate < 0.5:  # Less than 50% success rate
                    strategy_recommendations[strategy_id] = {
                        'action': 'review_and_adjust',
                        'reason': f'Low success rate: {success_rate:.2%}',
                        'suggested_changes': [
                            'Review task parameters',
                            'Check for systematic errors',
                            'Consider reducing task frequency'
                        ]
                    }
                elif success_rate > 0.8:  # Greater than 80% success rate
                    strategy_recommendations[strategy_id] = {
                        'action': 'scale_up',
                        'reason': f'High success rate: {success_rate:.2%}',
                        'suggested_changes': [
                            'Consider increasing task frequency',
                            'Expand strategy scope',
                            'Allocate more resources'
                        ]
                    }
            
            recommendations['strategy_adjustments'] = strategy_recommendations
            
            # Error-based recommendations
            error_recommendations = []
            for error_pattern in analyses.get('error_patterns', []):
                if error_pattern['count'] > 5:  # Frequent errors
                    error_recommendations.append({
                        'strategy_id': error_pattern['strategy_id'],
                        'error': error_pattern['error_message'],
                        'frequency': error_pattern['count'],
                        'action': 'investigate_and_fix'
                    })
            
            recommendations['error_fixes'] = error_recommendations
            
            self.logger.debug(f"Generated {len(recommendations)} recommendation categories")
            
        except Exception as e:
            self.logger.error(f"Failed to generate recommendations: {e}")
            recommendations = {}
        
        return recommendations
    
    async def _store_analysis_report(self, analyses: Dict[str, Any], recommendations: Dict[str, Any]):
        """Store analysis report in database for historical tracking."""
        try:
            current_time = asyncio.get_event_loop().time()
            analysis_start = current_time - 3600  # Last hour
            
            self.db_conn.execute("""
                INSERT INTO analytics_reports 
                (report_type, strategy_id, analysis_period_start, analysis_period_end, metrics, recommendations)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                'periodic_analysis',
                'all_strategies',
                analysis_start,
                current_time,
                json.dumps(analyses),
                json.dumps(recommendations)
            ))
            
            self.db_conn.commit()
            self.logger.debug("Stored analysis report in database")
            
        except Exception as e:
            self.logger.error(f"Failed to store analysis report: {e}")
    
    async def _publish_reflection_updates(self, recommendations: Dict[str, Any]):
        """
        Publish reflection updates to notify strategies of recommended changes.
        
        Args:
            recommendations: Generated recommendations for strategy updates
        """
        try:
            reflection_data = {
                'timestamp': asyncio.get_event_loop().time(),
                'type': 'analysis_based_reflection',
                'recommendations': recommendations
            }
            
            await self.message_bus.publish(
                Topics.REFLECTION_UPDATE,
                reflection_data,
                port=Ports.GLOBAL_EVENTS
            )
            
            self.logger.info(f"Published reflection updates for {len(recommendations)} recommendation types")
            
        except Exception as e:
            self.logger.error(f"Failed to publish reflection updates: {e}")
    
    async def cleanup(self):
        """Clean up analytics service resources."""
        await super().cleanup()
        await self.message_bus.cleanup()
        
        if self.db_conn:
            self.db_conn.close()
            self.logger.info("Closed analytics database connection")
        
        self.logger.info("DataAnalytics service cleaned up")


class DataAnalyticsMessageBus(MessageBus):
    """
    Custom MessageBus for DataAnalyticsService.
    
    Overrides message handling to process task results.
    """
    
    def __init__(self, service_name: str):
        super().__init__(service_name)
        self.analytics_service: Optional[DataAnalyticsService] = None
    
    def set_analytics_service(self, service: DataAnalyticsService):
        """Set reference to analytics service for result processing."""
        self.analytics_service = service
    
    async def _handle_pulled_message(self, message: Dict[str, Any]):
        """Handle pulled task results."""
        if self.analytics_service:
            await self.analytics_service.store_task_result(message.get('data', {}))
        else:
            self.logger.warning("No analytics service reference set")