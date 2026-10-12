"""
include/code/llm.py
------------------------
ONE function the pipeline calls:   ask_llm(prompt) -> str | None

Which LLM answers is decided by SETTINGS, not by code. To switch LLM you change
Airflow Variables (Admin -> Variables), no code change and no redeploy:

    LLM_PROVIDER      none | anthropic | openai | azure_openai | openai_compatible
    LLM_MODEL         model name (Azure: your *deployment* name)
    LLM_API_KEY       the key for that provider          <- secret: Variable, never in code/Git
    LLM_BASE_URL      only for azure_openai (endpoint) and openai_compatible (server URL)
    LLM_API_VERSION   only for azure_openai (default 2024-08-01-preview)

If a Variable is not set, an environment variable with the same name is used
(handy for run_local.py on your laptop).

Examples
  Claude (Anthropic)   LLM_PROVIDER=anthropic          LLM_MODEL=claude-opus-5-5 (default)
  OpenAI               LLM_PROVIDER=openai             LLM_MODEL=<your model>
  Azure OpenAI         LLM_PROVIDER=azure_openai       LLM_MODEL=<deployment>  LLM_BASE_URL=https://<res>.openai.azure.com
                       (falls back to the trainer's AZURE_OPENAI_* Variables if LLM_* are empty)
  Ollama (local, free) LLM_PROVIDER=openai_compatible  LLM_MODEL=llama3.1  LLM_BASE_URL=http://host.docker.internal:11434/v1
  Groq / OpenRouter / Gemini / LM Studio / vLLM: also openai_compatible, with that service's base URL.

Adding a brand-new provider = write one function `_call_<name>(prompt, system, settings)`
and add it to PROVIDERS at the bottom. Nothing else in the project changes.

Safety rules baked in
  - The pipeline must NEVER fail because an LLM is down: every error returns None,
    and the caller falls back to a plain template message.
  - Only aggregated counts and check names are sent - never customer rows or PII.
"""

import os

DEFAULT_MODELS = {
    "anthropic": "claude-opus-5-5",
}


# ── Settings ──────────────────────────────────────────────────────────────────

def _setting(name, default=None):
    """Airflow Variable first, then environment variable, then default."""
    try:
        from airflow.sdk import Variable           # Airflow 3 Task SDK
        value = Variable.get(name, default=None)
        if value:
            return value
    except Exception:
        pass                                       # not running inside Airflow (e.g. run_local.py)
    return os.getenv(name) or default


def load_settings():
    provider = (_setting("LLM_PROVIDER", "none") or "none").strip().lower()
    settings = {
        "provider": provider,
        "model": _setting("LLM_MODEL", DEFAULT_MODELS.get(provider)),
        "api_key": _setting("LLM_API_KEY"),
        "base_url": _setting("LLM_BASE_URL"),
        "api_version": _setting("LLM_API_VERSION", "2024-08-01-preview"),
    }
    # Re-use the trainer's Azure Variables (see include/ai_explainer.py) if LLM_* are empty
    if provider == "azure_openai":
        settings["api_key"] = settings["api_key"] or _setting("AZURE_OPENAI_API_KEY")
        settings["base_url"] = settings["base_url"] or _setting("AZURE_OPENAI_ENDPOINT")
        settings["model"] = settings["model"] or _setting("AZURE_OPENAI_DEPLOYMENT")
    return settings


# ── Providers ─────────────────────────────────────────────────────────────────
# Each provider imports its own SDK *inside* the function, so a package you don't
# use never needs to be installed, and the DAG still parses without it.

def _call_anthropic(prompt, system, s):
    """Claude via the official `anthropic` SDK (pip package: anthropic)."""
    import anthropic

    # api_key=None lets the SDK read ANTHROPIC_API_KEY from the environment
    client = anthropic.Anthropic(api_key=s["api_key"]) if s["api_key"] else anthropic.Anthropic()
    request = {
        "model": s["model"],
        "max_tokens": 4000,
        "system": system,
        "messages": [{"role": "user", "content": prompt}],
        # A short summary is a simple task -> low effort keeps it fast and cheap.
        "output_config": {"effort": "low"},
    }
    try:
        if s["model"] in ("claude-opus-5-5", "claude-opus-5", "claude-sonnet-5-5", "claude-fable-5-1"):
            # Server-side fallback: if the model declines, the API retries on a
            # suitable fallback model inside the same call.
            response = client.beta.messages.create(
                **request, betas=["server-side-fallback-2026-07-01"], fallbacks="default")
        else:
            response = client.messages.create(**request)
    except anthropic.AuthenticationError:
        print("[LLM] Anthropic: invalid API key (check the LLM_API_KEY Variable)")
        return None
    except anthropic.RateLimitError:
        print("[LLM] Anthropic: rate limited - using the template message this time")
        return None
    except anthropic.APIStatusError as e:
        print(f"[LLM] Anthropic API error {e.status_code}: {e.message}")
        return None
    except anthropic.APIConnectionError:
        print("[LLM] Anthropic: network error")
        return None

    if response.stop_reason == "refusal":
        print("[LLM] Anthropic declined the request")
        return None
    return "".join(b.text for b in response.content if b.type == "text").strip() or None


def _openai_chat(client, model, prompt, system):
    """Shared by every OpenAI-style provider (OpenAI, Azure, Ollama, Groq, ...)."""
    response = client.chat.completions.create(
        model=model,
        messages=[{"role": "system", "content": system}, {"role": "user", "content": prompt}],
    )
    return (response.choices[0].message.content or "").strip() or None


def _call_openai(prompt, system, s):
    from openai import OpenAI          # pip package: openai (also pulled in by langchain-openai)
    return _openai_chat(OpenAI(api_key=s["api_key"]), s["model"], prompt, system)


def _call_azure_openai(prompt, system, s):
    from openai import AzureOpenAI
    client = AzureOpenAI(api_key=s["api_key"], azure_endpoint=s["base_url"], api_version=s["api_version"])
    return _openai_chat(client, s["model"], prompt, system)


def _call_openai_compatible(prompt, system, s):
    """Any server that speaks the OpenAI chat API: Ollama, LM Studio, vLLM, Groq, OpenRouter, Gemini..."""
    from openai import OpenAI
    client = OpenAI(api_key=s["api_key"] or "not-needed", base_url=s["base_url"])
    return _openai_chat(client, s["model"], prompt, system)


PROVIDERS = {
    "anthropic": _call_anthropic,
    "openai": _call_openai,
    "azure_openai": _call_azure_openai,
    "openai_compatible": _call_openai_compatible,
}


# ── The one public function ───────────────────────────────────────────────────

def ask_llm(prompt, system="You are a concise data-quality assistant."):
    """
    Returns the model's text, or None if no LLM is configured or anything goes wrong.
    Callers must always have a non-LLM fallback.
    """
    s = load_settings()
    if s["provider"] in ("", "none"):
        print("[LLM] LLM_PROVIDER is 'none' - skipping (template message will be used)")
        return None
    call = PROVIDERS.get(s["provider"])
    if call is None:
        print(f"[LLM] Unknown LLM_PROVIDER '{s['provider']}'. Choose one of: none, {', '.join(PROVIDERS)}")
        return None
    if not s["model"]:
        print(f"[LLM] LLM_MODEL is not set for provider '{s['provider']}'")
        return None

    print(f"[LLM] asking provider={s['provider']} model={s['model']}")
    try:
        return call(prompt, system, s)
    except ImportError as e:
        print(f"[LLM] SDK not installed for '{s['provider']}': {e}. Add it to requirements.txt")
    except Exception as e:                       # never let an LLM problem fail the pipeline
        print(f"[LLM] {s['provider']} call failed: {type(e).__name__}: {e}")
    return None
