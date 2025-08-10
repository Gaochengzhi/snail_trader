# Snail Trader Project

## Package Management
- This project uses **uv** for package management
- To add new packages: `source .venv/bin/activate && uv add package_name`
- Always activate virtual environment first before adding packages
- **IMPORTANT**: Project is installed as a global package, so **DO NOT** use `sys.path.append()` in any code files

## Configuration System
- Uses YAML configuration files in `configs/` directory
- Base configuration: `configs/base.yaml`
- Environment-specific configs: `configs/{env}.yaml` (e.g., `configs/ollama.yaml`)
- Environment variables are resolved from `.env` file using `${VAR_NAME}` syntax

## Documentation Organization Rules
- **Component usage instructions**: Store in the component's folder README.md (e.g., `test/README.md` for testing), Every time you update componet file you should update componet's README.md as well.
- **Project-wide rules**: Keep in root `CLAUDE.md` 
- **Exception**: Notebooks (`notebook/`) are self-contained and don't need separate README files
- **Examples**: 
  - Testing methods → `test/README.md`
  - API usage → `llm_api/README.md`
  - Utils usage → `utils/README.md`

## Dependencies
- python-dotenv: for .env file support
- pydantic: for configuration validation
- PyYAML: for YAML configuration files
- fire: for elegant CLI interface