import os
from dotenv import load_dotenv

# Search for .env in the current directory and its parent (backend/ or root)
load_dotenv()
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

LLM_API_KEY = os.getenv("LLM_API_KEY") or os.getenv("GEMINI_API_KEY")
LLM_MODEL = os.getenv("LLM_MODEL") or os.getenv("GEMINI_MODEL", "llama-3.3-71b-versatile")
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "groq" if (LLM_API_KEY and LLM_API_KEY.startswith("gsk_")) else "gemini")

# DEBUG: Check if we actually found a key
if LLM_API_KEY:
    masked_key = f"{LLM_API_KEY[:6]}...{LLM_API_KEY[-4:]}"
    print(f"[LLM] ({LLM_PROVIDER.upper()}) Loaded API KEY: {masked_key}")
else:
    print("[LLM] WARNING: No API KEY found in environment or .env file!")

print(f"[LLM] Active Model: {LLM_MODEL}")

