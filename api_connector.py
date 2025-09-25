"""Handles the interaction with external Generative AI APIs.

This module contains the function responsible for taking a finalized prompt,
constructing the appropriate API request for the configured AI provider
(e.g., Google, OpenAI), sending the request, and parsing the response.
"""

import os
import json
import requests
import time
import logging
import re

logger = logging.getLogger(__name__)

def translate_code_with_connector(settings, full_prompt, system_instruction):
    """
    Handles the AI translation workflow by sending a prompt to an AI service.

    This function reads AI provider details from the settings, builds a
    provider-specific payload, and sends the prompt via an HTTP POST request.
    It returns the translated code and metrics.

    Args:
        settings (dict): The main application settings dictionary.
        full_prompt (str): The complete, formatted prompt string for the user role.
        system_instruction (str): The system prompt or instruction for the model.

    Returns:
        tuple: (translated code, metrics dictionary with processing_time_seconds and prompt_estimated_tokens)
    """
    ai_provider = settings.get('ai_provider', 'google')
    api_endpoint = settings.get('ai_endpoint')
    ai_model = settings.get('ai_model')

    # Prioritize environment variable for API key for better security
    api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("OPENAI_API_KEY") or settings.get('ai_api_key')

    if not api_key:
        logger.error("AI API key not found in settings or environment variables")
        return "Error: Missing AI API key", {'processing_time_seconds': 0, 'prompt_estimated_tokens': 0}

    try:
        headers = {'Content-Type': 'application/json'}
        prompt_chars = len(full_prompt) + len(system_instruction)
        estimated_tokens = prompt_chars // 4  # Rough estimate: 4 chars per token

        # SSL settings
        verify_ssl = settings.get('ai_ssl_verify', True)
        ssl_cert_path = settings.get('ssl_cert_path', '')
        if verify_ssl and ssl_cert_path:
            verify_ssl = ssl_cert_path

        # Google Gemini API structure
        if "generativelanguage.googleapis.com" in api_endpoint:
            api_url = f"{api_endpoint}{ai_model}:generateContent?key={api_key}"
            payload = {
                "contents": [{"parts": [{"text": full_prompt}]}],
                "systemInstruction": {"parts": [{"text": system_instruction}]},
                "generationConfig": {
                    "temperature": float(settings.get('ai_temperature', 0.2)),
                    "maxOutputTokens": int(settings.get('ai_max_output_tokens', 8192))
                }
            }
        # OpenAI or compatible API structure
        else:
            api_url = f"{api_endpoint}v1/chat/completions"
            headers['Authorization'] = f'Bearer {api_key}'
            payload = {
                "model": ai_model,
                "messages": [
                    {"role": "system", "content": system_instruction},
                    {"role": "user", "content": full_prompt}
                ],
                "temperature": float(settings.get('ai_temperature', 0.2)),
                "max_tokens": int(settings.get('ai_max_output_tokens', 4096)),
            }

        logger.info(f"Calling AI endpoint: {ai_provider.capitalize()} ({ai_model})")
        start_time = time.time()

        response = requests.post(api_url, headers=headers, data=json.dumps(payload), verify=verify_ssl)
        response.raise_for_status()
        result = response.json()

        duration = time.time() - start_time
        logger.info(f"AI processing time: {duration:.2f} seconds")

        if "generativelanguage.googleapis.com" in api_endpoint:
            translation = result['candidates'][0]['content']['parts'][0]['text']
        else:
            translation = result['choices'][0]['message']['content']

        # Clean up markdown fences if the model includes them
        translation = re.sub(r'^```sql\n', '', translation, flags=re.MULTILINE)
        translation = re.sub(r'\n```$', '', translation, flags=re.MULTILINE)

        return translation.strip(), {
            'processing_time_seconds': duration,
            'prompt_estimated_tokens': estimated_tokens
        }

    except requests.exceptions.RequestException as e:
        error_text = e.response.text if e.response else 'No response'
        logger.error(f"API request failed: {e}\nResponse: {error_text}")
        return f"Error: API request failed: {str(e)}", {'processing_time_seconds': 0, 'prompt_estimated_tokens': 0}
    except (KeyError, IndexError) as e:
        logger.error(f"Could not parse AI response. Unexpected structure: {e}\nResponse JSON: {result}")
        return f"Error: Invalid AI response format: {str(e)}", {'processing_time_seconds': 0, 'prompt_estimated_tokens': 0}
    except Exception as e:
        logger.error(f"Unexpected error in AI connector: {e}")
        return f"Error: Unexpected issue in AI processing: {str(e)}", {'processing_time_seconds': 0, 'prompt_estimated_tokens': 0}
