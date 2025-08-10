# Testing

## Configuration Testing

### Fire CLI Usage

```bash
# Show configuration details
python test/test_config.py show --config_name ollama
python test/test_config.py show --config_name base

# Test both configurations
python test/test_config.py test

# Get LLM instance with specific config
python test/test_config.py llm --config ollama
```

### Available Configurations

- **base**: Multi-provider configuration with API fallback
  - gpt_free (OpenRouter)  
  - gpt_light (Gemini Flash)
  - gpt_pro (Gemini Pro)
  - ollama (local)

- **ollama**: Pure local Ollama configuration
  - ollama only with qwen2.5:32b model

### Test Files

- `test_config.py`: Configuration testing and CLI interface
- `../notebook/llm_test.ipynb`: Interactive testing notebook

### Environment Variables

Make sure `.env` file exists with required API keys:
```
GEMINI_API_KEY=your_gemini_key
OPENROUTER_API_KEY=your_openrouter_key
```