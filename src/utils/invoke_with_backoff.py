import time
from src.utils.logging_config import LOG
from src.utils.constants import *

def invoke_with_backoff(chain, payload, max_retries=5, base_delay=6):
    """
    Esegue chain.invoke() con exponential backoff se la risposta contiene errori di quota.
    """
    for attempt in range(1, max_retries + 1):
        try:
            response = chain.invoke(payload)

            # se il modello risponde con un messaggio di errore nel testo
            if isinstance(response, str) and QUOTA_ERROR_REGEX.search(response):
                raise RuntimeError(f"LLM error detected in response: {response}")

            return response

        except Exception as e:
            msg = str(e)

            # se NON è un errore di quota → rilancia subito
            if not QUOTA_ERROR_REGEX.search(msg):
                raise

            # calcola delay esponenziale
            delay = base_delay * (2 ** (attempt - 1))
            LOG.warning(f"[BACKOFF] Attempt {attempt}/{max_retries} failed due to quota: {msg}. "
                        f"Retrying in {delay:.1f}s...")
            time.sleep(delay)

    raise RuntimeError(f"Exceeded maximum retries ({max_retries}) due to persistent quota errors.")
