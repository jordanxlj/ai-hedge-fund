"""Helper functions for LLM"""

import json
import hashlib
from pydantic import BaseModel
from src.llm.models import get_model, get_model_info
from src.utils.progress import progress
from src.graph.state import AgentState
from src.data.persistent_cache import get_persistent_cache


def call_llm(
    prompt: any,
    pydantic_model: type[BaseModel],
    agent_name: str | None = None,
    state: AgentState | None = None,
    max_retries: int = 3,
    default_factory=None,
) -> BaseModel:
    """
    Makes an LLM call with retry logic, handling both JSON supported and non-JSON supported models.
    For DeepSeek models, responses are cached to improve performance.

    Args:
        prompt: The prompt to send to the LLM
        pydantic_model: The Pydantic model class to structure the output
        agent_name: Optional name of the agent for progress updates and model config extraction
        state: Optional state object to extract agent-specific model configuration
        max_retries: Maximum number of retries (default: 3)
        default_factory: Optional factory function to create default response on failure

    Returns:
        An instance of the specified Pydantic model
    """
    
    # Extract model configuration if state is provided and agent_name is available
    model_name = None
    model_provider = None
    if state and agent_name:
        model_name, model_provider = get_agent_model_config(state, agent_name)
    
    # Fallback to defaults if still not provided
    if not model_name:
        model_name = "gpt-4o"
    if not model_provider:
        model_provider = "OPENAI"

    # Check if this is a DeepSeek model and use cache
    is_deepseek = model_provider == "DeepSeek" or (model_name and model_name.startswith("deepseek"))
    
    if is_deepseek:
        # Generate cache key based on prompt, model, and pydantic model
        cache_key = _generate_llm_cache_key(prompt, model_name, pydantic_model.__name__, agent_name)
        
        # Try to get cached response
        cached_response = _get_cached_llm_response(cache_key)
        if cached_response:
            try:
                return pydantic_model(**cached_response)
            except Exception as e:
                print(f"Error loading cached response: {e}")
                # Fall through to make fresh API call

    model_info = get_model_info(model_name, model_provider)
    llm = get_model(model_name, model_provider)

    # For non-JSON support models, we can use structured output
    if not (model_info and not model_info.has_json_mode()):
        llm = llm.with_structured_output(
            pydantic_model,
            method="json_mode",
        )

    # Call the LLM with retries
    for attempt in range(max_retries):
        try:
            # Call the LLM
            result = llm.invoke(prompt)

            # For non-JSON support models, we need to extract and parse the JSON manually
            if model_info and not model_info.has_json_mode():
                parsed_result = extract_json_from_response(result.content)
                if parsed_result:
                    final_result = pydantic_model(**parsed_result)
                    # Only cache successful responses for DeepSeek models
                    if is_deepseek and hasattr(final_result, 'model_dump'):
                        _cache_llm_response(cache_key, final_result.model_dump())
                    return final_result
                else:
                    # Debug: print the actual response content for troubleshooting
                    print(f"Debug: Failed to extract JSON from DeepSeek response: {repr(result.content[:200])}")
                    raise ValueError("Failed to extract JSON from response")
            else:
                final_result = result
                # Cache the response for DeepSeek models (JSON mode supported models)
                if is_deepseek and hasattr(final_result, 'model_dump'):
                    _cache_llm_response(cache_key, final_result.model_dump())
                return final_result

        except Exception as e:
            if agent_name:
                progress.update_status(agent_name, None, f"Error - retry {attempt + 1}/{max_retries}")

            if attempt == max_retries - 1:
                print(f"Error in LLM call after {max_retries} attempts: {e}")
                # Use default_factory if provided, otherwise create a basic default
                if default_factory:
                    return default_factory()
                return create_default_response(pydantic_model)

    # This should never be reached due to the retry logic above
    return create_default_response(pydantic_model)


def create_default_response(model_class: type[BaseModel]) -> BaseModel:
    """Creates a safe default response based on the model's fields."""
    default_values = {}
    for field_name, field in model_class.model_fields.items():
        if field.annotation == str:
            default_values[field_name] = "Error in analysis, using default"
        elif field.annotation == float:
            default_values[field_name] = 0.0
        elif field.annotation == int:
            default_values[field_name] = 0
        elif hasattr(field.annotation, "__origin__") and field.annotation.__origin__ == dict:
            default_values[field_name] = {}
        else:
            # For other types (like Literal), try to use the first allowed value
            if hasattr(field.annotation, "__args__"):
                default_values[field_name] = field.annotation.__args__[0]
            else:
                default_values[field_name] = None

    return model_class(**default_values)


def _generate_llm_cache_key(prompt: any, model_name: str, pydantic_model_name: str, agent_name: str = None) -> str:
    """Generate a unique cache key for LLM requests."""
    # Convert prompt to string for hashing
    if isinstance(prompt, list) and all(hasattr(msg, 'content') for msg in prompt):
        # List of BaseMessage objects
        prompt_str = str([msg.content for msg in prompt])
    elif hasattr(prompt, 'messages'):
        # ChatPromptTemplate case
        prompt_str = str([msg.content if hasattr(msg, 'content') else str(msg) for msg in prompt.messages])
    elif hasattr(prompt, '__str__'):
        prompt_str = str(prompt)
    else:
        prompt_str = repr(prompt)
    
    # Create cache key components
    components = [
        prompt_str,
        model_name,
        pydantic_model_name,
        agent_name or "unknown"
    ]
    
    # Generate hash
    combined = "_".join(components)
    return hashlib.md5(combined.encode()).hexdigest()


def _get_cached_llm_response(cache_key: str) -> dict | None:
    """Get cached LLM response if available."""
    try:
        cache = get_persistent_cache()
        cached_data = cache.get('llm_responses', cache_key=cache_key)
        if cached_data and len(cached_data) > 0:
            return cached_data[0].get('response')
    except Exception as e:
        print(f"Error reading from LLM cache: {e}")
    return None


def _cache_llm_response(cache_key: str, response_data: dict):
    """Cache LLM response for future use."""
    try:
        from src.data.data_config import get_cache_ttl
        cache = get_persistent_cache()
        cache_data = [{
            'cache_key': cache_key,
            'response': response_data,
            'timestamp': str(hashlib.md5(cache_key.encode()).hexdigest())  # Use hash as timestamp placeholder
        }]
        # Use TTL from configuration
        ttl = get_cache_ttl('llm_responses')
        cache.set('llm_responses', cache_data, ttl=ttl, cache_key=cache_key)
    except Exception as e:
        print(f"Error caching LLM response: {e}")


def extract_json_from_response(content: str) -> dict | None:
    """Extracts JSON from various response formats with error correction."""
    if not content:
        return None
        
    content = content.strip()
    
    def clean_json_string(json_str: str) -> str:
        """Clean common JSON formatting issues."""
        # Fix common escape sequence issues
        json_str = json_str.replace("\\'", "'")  # Fix incorrect single quote escaping
        json_str = json_str.replace("\\n", "\\\\n")  # Fix newline escaping
        json_str = json_str.replace("\n", "\\n")  # Escape unescaped newlines
        json_str = json_str.replace("\t", "\\t")  # Escape unescaped tabs
        json_str = json_str.replace("\r", "\\r")  # Escape unescaped carriage returns
        
        # Remove any control characters that might cause issues
        import re
        json_str = re.sub(r'[\x00-\x1F\x7F]', '', json_str)
        
        return json_str
    
    def try_parse_json(json_str: str) -> dict | None:
        """Try to parse JSON with cleaning."""
        try:
            return json.loads(json_str)
        except json.JSONDecodeError:
            try:
                # Try with cleaning
                cleaned = clean_json_string(json_str)
                return json.loads(cleaned)
            except json.JSONDecodeError:
                return None
    
    try:
        # Try 1: Direct JSON parsing (if response is pure JSON)
        result = try_parse_json(content)
        if result:
            return result
        
        # Try 2: Extract from markdown code block
        json_start = content.find("```json")
        if json_start != -1:
            json_text = content[json_start + 7:]  # Skip past ```json
            json_end = json_text.find("```")
            if json_end != -1:
                json_text = json_text[:json_end].strip()
                result = try_parse_json(json_text)
                if result:
                    return result
        
        # Try 3: Extract from generic code block
        code_start = content.find("```")
        if code_start != -1:
            code_text = content[code_start + 3:]  # Skip past ```
            code_end = code_text.find("```")
            if code_end != -1:
                code_text = code_text[:code_end].strip()
                # Skip language identifier if present
                if code_text.startswith(('json', 'javascript', 'js')):
                    lines = code_text.split('\n', 1)
                    code_text = lines[1] if len(lines) > 1 else code_text
                result = try_parse_json(code_text)
                if result:
                    return result
        
        # Try 4: Find JSON object within text
        brace_start = content.find("{")
        if brace_start != -1:
            # Find the matching closing brace
            brace_count = 0
            for i, char in enumerate(content[brace_start:], brace_start):
                if char == "{":
                    brace_count += 1
                elif char == "}":
                    brace_count -= 1
                    if brace_count == 0:
                        potential_json = content[brace_start:i+1]
                        result = try_parse_json(potential_json)
                        if result:
                            return result
                        break
                        
    except Exception as e:
        print(f"Error extracting JSON from response: {e}")
    
    return None


def get_agent_model_config(state, agent_name):
    """
    Get model configuration for a specific agent from the state.
    Falls back to global model configuration if agent-specific config is not available.
    """
    request = state.get("metadata", {}).get("request")

    if agent_name == 'portfolio_manager':
        # Get the model and provider from state metadata
        model_name = state.get("metadata", {}).get("model_name", "gpt-4o")
        model_provider = state.get("metadata", {}).get("model_provider", "OPENAI")
        return model_name, model_provider
    
    if request and hasattr(request, 'get_agent_model_config'):
        # Get agent-specific model configuration
        model_name, model_provider = request.get_agent_model_config(agent_name)
        return model_name, model_provider.value if hasattr(model_provider, 'value') else str(model_provider)
    
    # Fall back to global configuration
    model_name = state.get("metadata", {}).get("model_name", "gpt-4o")
    model_provider = state.get("metadata", {}).get("model_provider", "OPENAI")
    
    # Convert enum to string if necessary
    if hasattr(model_provider, 'value'):
        model_provider = model_provider.value
    
    return model_name, model_provider
