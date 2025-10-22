import asyncio
import aiohttp
import base58
import base64
import json
import logging
import os
import socket
import time
from typing import Dict, List
from solders.keypair import Keypair as SoldersKeypair
from solders.transaction import VersionedTransaction


# Configure logger
logger = logging.getLogger("bulk_buy")
logger.setLevel(logging.INFO)
if not logger.handlers:
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter('%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s'))
    logger.addHandler(ch)


# Defaults
TOKENS_FILE = "tokens.txt"
BUY_CONFIG = "buy_config.json"


def load_json(path: str) -> Dict:
    with open(path, "r") as f:
        return json.load(f)


def read_mints(tokens_file: str) -> List[str]:
    mints: List[str] = []
    with open(tokens_file, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            token = line.strip().strip('.,;')
            try:
                _ = base58.b58decode(token)
                if 32 <= len(token) <= 60:
                    mints.append(token)
                else:
                    logger.warning(f"Skipping invalid mint (length): {token}")
            except Exception:
                logger.warning(f"Skipping invalid base58 mint: {token}")
    return mints


async def jupiter_ultra_order_execute(
    session: aiohttp.ClientSession,
    kp: SoldersKeypair,
    mint_out: str,
    *,
    amount_sol: float,
    slippage_bps: int,
    safe_time: float,
    ultra_base_url: str = "https://lite-api.jup.ag/ultra",
    compute_unit_price_micro_lamports: int | None = None,
) -> Dict:
    # Optional delay before starting (set to 0 for fastest execution)
    if safe_time and safe_time > 0:
        await asyncio.sleep(safe_time)

    user_pubkey = str(kp.pubkey())
    input_mint = "So11111111111111111111111111111111111111112"
    amount_lamports = int(amount_sol * 1_000_000_000)

    # 1) Get Order
    order_url = (
        f"{ultra_base_url.rstrip('/')}/v1/order"
        f"?inputMint={input_mint}"
        f"&outputMint={mint_out}"
        f"&amount={amount_lamports}"
       # f"&slippageBps={slippage_bps}"
       # f"&swapMode=ExactIn"
        f"&taker={user_pubkey}"
    )
    
    # Log the exact parameters being passed to the ORDER API call
    order_params = {
        "inputMint": input_mint,
        "outputMint": mint_out,
        "amount": amount_lamports,
        "slippageBps": slippage_bps,
        "swapMode": "ExactIn",
        "taker": user_pubkey,
    }
    logger.info(f"[ORDER_PARAMS] {order_params}")
    # Log headers (masked) used for the ORDER request
    try:
        api_key_val = None
        # Prefer public attribute if available; fallback to private
        if hasattr(session, 'headers') and session.headers is not None:
            api_key_val = session.headers.get('x-api-key') or session.headers.get('X-API-Key')
        if not api_key_val and hasattr(session, '_default_headers'):
            api_key_val = session._default_headers.get('x-api-key') or session._default_headers.get('X-API-Key')
    except Exception:
        api_key_val = None
    masked_key = None
    if isinstance(api_key_val, str) and len(api_key_val) > 4:
        masked_key = f"***{api_key_val[-4:]}"
    elif api_key_val:
        masked_key = "***set***"
    headers_log = {"x-api-key": masked_key if api_key_val else None}
    logger.info(f"[ORDER_HEADERS] {headers_log}")
    logger.info(f"[ORDER] mint={mint_out[:8]}... amount={amount_sol} SOL slippage={slippage_bps}bps")
    
    try:
        order = await http_json(session, "GET", order_url, timeout=15)
    except Exception as e:
        logger.error(f"[ORDER_EXCEPTION] mint={mint_out[:8]} err={type(e).__name__}: {str(e)}")
        return {"mint": mint_out, "status": "order_exception", "error": str(e)}
    
    order_time = time.monotonic()
    
    if isinstance(order, dict) and order.get("_error"):
        logger.error(f"[ORDER_ERROR] mint={mint_out[:8]} err={order.get('_error')} detail={order.get('_detail')}")
        return {"mint": mint_out, "status": "order_error", "error": order.get("_error"), "detail": order.get("_detail")}
    
    if not order or not isinstance(order, dict):
        logger.error(f"[ORDER_EMPTY] mint={mint_out[:8]} response={order}")
        return {"mint": mint_out, "status": "order_empty", "response": str(order)[:500]}

    request_id = order.get("requestId")
    order_tx_b64 = order.get("transaction")
    
    logger.info(f"[ORDER_RECEIVED] request_id={request_id is not None} has_tx={order_tx_b64 is not None}")
    
    if not request_id:
        logger.error(f"[ORDER_NO_REQUEST_ID] order_keys={list(order.keys())}")
        return {"mint": mint_out, "status": "order_missing_request_id", "order": order}
    if not order_tx_b64:
        logger.error(f"[ORDER_NO_TX] order={order}")
        return {"mint": mint_out, "status": "order_missing_transaction", "order": order}

    # 2) Sign and Execute IMMEDIATELY
    try:
        # Deserialize
        transaction_bytes = base64.b64decode(order_tx_b64)
        transaction = VersionedTransaction.from_bytes(transaction_bytes)
        
        # Sign
        if hasattr(transaction, 'message_data'):
            msg_bytes = transaction.message_data()
        else:
            msg_bytes = bytes(transaction.message)
        
        signature = kp.sign_message(msg_bytes)
        signed_tx = VersionedTransaction.populate(transaction.message, [signature])
        signed_transaction_b64 = base64.b64encode(bytes(signed_tx)).decode('utf-8')
        
        logger.info(f"[SIGNED] tx_size={len(signed_transaction_b64)} chars")
        
        # Execute with longer timeout and better error handling
        exec_url = f"{ultra_base_url.rstrip('/')}/v1/execute"
        exec_body = {
            'signedTransaction': signed_transaction_b64,
            'requestId': request_id,
        }
       
        # Log a safe summary of the EXECUTE params (do not print full signed tx)
        stx = signed_transaction_b64 or ""
        safe_exec_params = {
            'requestId': request_id,
            'signedTransaction': {
                'len': len(stx),
                'prefix': stx[:16] + ("..." if len(stx) > 16 else ""),
            }
        }
        logger.info(f"[EXECUTE_PARAMS] {safe_exec_params}")
        
        # Log headers (masked) for the EXECUTE request
        try:
            api_key_val2 = None
            if hasattr(session, 'headers') and session.headers is not None:
                api_key_val2 = session.headers.get('x-api-key') or session.headers.get('X-API-Key')
            if not api_key_val2 and hasattr(session, '_default_headers'):
                api_key_val2 = session._default_headers.get('x-api-key') or session._default_headers.get('X-API-Key')
        except Exception:
            api_key_val2 = None
        masked_key2 = None
        if isinstance(api_key_val2, str) and len(api_key_val2) > 4:
            masked_key2 = f"***{api_key_val2[-4:]}"
        elif api_key_val2:
            masked_key2 = "***set***"
        headers_log2 = {"x-api-key": masked_key2 if api_key_val2 else None}
        logger.info(f"[EXECUTE_HEADERS] {headers_log2}")
        logger.info(f"[EXECUTING] Sending to {exec_url}")
        
        # Try execute with 30 second timeout
        try:
            async with session.post(
                exec_url, 
                json=exec_body, 
                timeout=aiohttp.ClientTimeout(total=30, connect=10)
            ) as resp:
                resp_status = resp.status
                resp_text = await resp.text()
                
                logger.info(f"[EXECUTE_RAW] status={resp_status} body_len={len(resp_text)}")
                
                if resp_status != 200:
                    logger.error(f"[EXECUTE_HTTP_ERROR] status={resp_status} body={resp_text[:500]}")
                    return {
                        "mint": mint_out, 
                        "status": "execute_http_error", 
                        "http_status": resp_status,
                        "error": resp_text[:500]
                    }
                
                try:
                    execute_response = json.loads(resp_text)
                except json.JSONDecodeError as je:
                    logger.error(f"[EXECUTE_JSON_ERROR] {str(je)} body={resp_text[:200]}")
                    return {
                        "mint": mint_out,
                        "status": "execute_json_error",
                        "error": str(je),
                        "body": resp_text[:200]
                    }
        
        except asyncio.TimeoutError:
            logger.error(f"[EXECUTE_TIMEOUT] Request timed out after 30s")
            return {"mint": mint_out, "status": "execute_timeout", "error": "Request timed out"}
        except aiohttp.ClientError as ce:
            logger.error(f"[EXECUTE_CLIENT_ERROR] {type(ce).__name__}: {str(ce)}")
            return {"mint": mint_out, "status": "execute_client_error", "error": str(ce)}
        
        exec_time = time.monotonic() - order_time
        logger.info(f"[EXECUTE_DONE] time={exec_time:.2f}s response={execute_response}")
        
        status = execute_response.get('status')
        sig = execute_response.get('signature') or execute_response.get('txid')
        code = execute_response.get('code')
        
        if status == 'Success' and sig:
            logger.info(f"✓ SUCCESS {mint_out[:8]}... | https://solscan.io/tx/{sig}")
            return {"mint": mint_out, "status": "success", "signature": sig}
        
        # Check if we should retry
        too_slow = exec_time > 2.5
        expired = code == -1005
        
        if expired or too_slow:
            reason = 'expired' if expired else 'slow'
            logger.info(f"[RETRY] reason={reason} time={exec_time:.2f}s - getting fresh order")
            
            try:
                # Get fresh order
                order2 = await http_json(session, "GET", order_url, timeout=10)
                order_time2 = time.monotonic()
                
                if not isinstance(order2, dict):
                    return {"mint": mint_out, "status": "retry_failed", "error": "invalid_order2"}
                
                request_id2 = order2.get("requestId")
                order_tx_b64_2 = order2.get("transaction")
                
                if not request_id2 or not order_tx_b64_2:
                    return {"mint": mint_out, "status": "retry_failed", "error": "missing_order2_data"}
                
                # Sign fresh transaction
                tx2 = VersionedTransaction.from_bytes(base64.b64decode(order_tx_b64_2))
                msg_bytes2 = tx2.message_data() if hasattr(tx2, 'message_data') else bytes(tx2.message)
                sig2 = kp.sign_message(msg_bytes2)
                signed_tx2 = VersionedTransaction.populate(tx2.message, [sig2])
                signed_tx_b64_2 = base64.b64encode(bytes(signed_tx2)).decode('utf-8')
                
                exec_body2 = {
                    'signedTransaction': signed_tx_b64_2,
                    'requestId': request_id2,
                }
                if compute_unit_price_micro_lamports is not None:
                    exec_body2['computeUnitPriceMicroLamports'] = int(compute_unit_price_micro_lamports)
                
                # Execute retry
                async with session.post(
                    exec_url, 
                    json=exec_body2, 
                    timeout=aiohttp.ClientTimeout(total=30, connect=10)
                ) as resp2:
                    execute_response2 = await resp2.json()
                
                exec_time2 = time.monotonic() - order_time2
                logger.info(f"[RETRY_EXEC] time={exec_time2:.2f}s response={execute_response2}")
                
                status2 = execute_response2.get('status')
                sig_final = execute_response2.get('signature') or execute_response2.get('txid')
                
                if status2 == 'Success' and sig_final:
                    logger.info(f"✓ SUCCESS (retry) {mint_out[:8]}... | https://solscan.io/tx/{sig_final}")
                    return {"mint": mint_out, "status": "success", "signature": sig_final}
                
                return {
                    "mint": mint_out, 
                    "status": "retry_failed", 
                    "error": execute_response2.get('error') or execute_response2.get('code'),
                    "detail": execute_response2
                }
            except Exception as retry_err:
                logger.error(f"[RETRY_EXCEPTION] {type(retry_err).__name__}: {str(retry_err)}")
                return {"mint": mint_out, "status": "retry_exception", "error": str(retry_err)}
        
        # Failed without retry conditions
        return {
            "mint": mint_out, 
            "status": "failed", 
            "error": execute_response.get('error') or code,
            "detail": execute_response
        }
        
    except Exception as e:
        import traceback
        error_detail = traceback.format_exc()
        logger.error(f"[SIGN_ERROR] mint={mint_out[:8]} type={type(e).__name__} err={str(e)}")
        logger.error(f"[SIGN_ERROR_TRACE] {error_detail}")
        return {"mint": mint_out, "status": "sign_error", "error": f"{type(e).__name__}: {str(e)}"}


async def runner(concurrency: int = 3, dry_run: bool = False) -> List[Dict]:
    # Create default config if missing
    if not os.path.exists(BUY_CONFIG):
        try:
            with open(BUY_CONFIG, "w") as bc:
                bc.write("{\n")
                bc.write("  \"ultra_base_url\": \"https://lite-api.jup.ag/ultra\",\n")
                bc.write("  \"wallet_secret_key\": \"REPLACE_WITH_BASE58_PRIVATE_KEY\",\n")
                bc.write("  \"amount_sol\": 0.01,\n")
                bc.write("  \"slippage\": 45,\n")
                bc.write("  \"safe_time\": 0.0,\n")
                bc.write("  \"ultra_api_key\": null,\n")
                bc.write("  \"compute_unit_price_micro_lamports\": null\n")
                bc.write("}\n")
            logger.info(f"Created default {BUY_CONFIG}. Please configure it.")
        except Exception as e:
            logger.error(f"Failed to create {BUY_CONFIG}: {e}")
            return []

    buy_cfg = load_json(BUY_CONFIG)

    # Load tokens
    try:
        mints = read_mints(TOKENS_FILE)
    except Exception as e:
        logger.error(f"Failed to read {TOKENS_FILE}: {e}")
        return []

    if not mints:
        logger.warning("No mints found in tokens file.")
        return []

    # Load wallet
    wallet_secret = buy_cfg.get("wallet_secret_key")
    if not wallet_secret:
        raise RuntimeError("wallet_secret_key missing in buy_config.json")

    # Parse keypair
    kp: SoldersKeypair
    if isinstance(wallet_secret, str):
        try:
            secret_bytes = base58.b58decode(wallet_secret)
            if len(secret_bytes) == 64:
                kp = SoldersKeypair.from_bytes(secret_bytes)
            elif len(secret_bytes) == 32:
                kp = SoldersKeypair.from_seed(secret_bytes)
            else:
                raise ValueError(f"Invalid secret key length: {len(secret_bytes)}")
        except Exception as e:
            raise RuntimeError(f"Invalid base58 wallet_secret_key: {e}")
    elif isinstance(wallet_secret, list):
        try:
            secret_bytes = bytes(wallet_secret)
            if len(secret_bytes) == 64:
                kp = SoldersKeypair.from_bytes(secret_bytes)
            elif len(secret_bytes) == 32:
                kp = SoldersKeypair.from_seed(secret_bytes)
            else:
                raise ValueError(f"Invalid secret key length: {len(secret_bytes)}")
        except Exception as e:
            raise RuntimeError(f"Invalid array wallet_secret_key: {e}")
    else:
        raise RuntimeError("wallet_secret_key must be base58 string or array")

    # Config params
    amount_sol = float(buy_cfg.get("amount_sol"))
    slippage_val = float(buy_cfg.get("slippage", 45))
    slippage_bps = int(slippage_val * 100) if slippage_val <= 100 else int(slippage_val)
    safe_time = float(buy_cfg.get("safe_time", 0.0))
    ultra_base_url = str(buy_cfg.get("ultra_base_url", "https://lite-api.jup.ag/ultra")).rstrip('/')
    ultra_api_key = buy_cfg.get("ultra_api_key")
    compute_unit_price_micro_lamports = buy_cfg.get("compute_unit_price_micro_lamports")
    
    if compute_unit_price_micro_lamports is not None:
        try:
            compute_unit_price_micro_lamports = int(compute_unit_price_micro_lamports)
        except Exception:
            logger.warning("Invalid compute_unit_price_micro_lamports; ignoring")
            compute_unit_price_micro_lamports = None

    logger.info(f"[CONFIG] Loaded {len(mints)} mints | amount={amount_sol} SOL | slippage={slippage_bps}bps")
    logger.info(f"[CONFIG] Ultra URL: {ultra_base_url} | API key: {'set' if ultra_api_key else 'none'}")
    
    # Test API key by making a simple request
    if ultra_api_key:
        logger.info(f"[CONFIG] Testing API key...")

    results: List[Dict] = []
    sem = asyncio.Semaphore(max(1, concurrency))

    # Use longer timeouts for connections
    connector = aiohttp.TCPConnector(
        family=socket.AF_INET,
        ttl_dns_cache=300,
        limit=100,
        limit_per_host=30
    )
    default_headers = {"x-api-key": ultra_api_key} if ultra_api_key else {}

    async with aiohttp.ClientSession(
        connector=connector, 
        headers=default_headers,
        timeout=aiohttp.ClientTimeout(total=60, connect=10)
    ) as session:
        async def bound_buy(mint: str):
            async with sem:
                logger.info(f"[START] {mint[:8]}...")
                res = await jupiter_ultra_order_execute(
                    session,
                    kp,
                    mint,
                    amount_sol=amount_sol,
                    slippage_bps=slippage_bps,
                    safe_time=safe_time,
                    ultra_base_url=ultra_base_url,
                    compute_unit_price_micro_lamports=compute_unit_price_micro_lamports,
                )
                if res.get("status") == "success":
                    logger.info(f"[OK] {mint[:8]}... sig={res.get('signature')}")
                else:
                    logger.error(f"[FAIL] {mint[:8]}... status={res.get('status')} err={res.get('error')}")
                results.append(res)

        await asyncio.gather(*(bound_buy(m) for m in mints))
        return results


async def http_json(session: aiohttp.ClientSession, method: str, url: str, *, json_payload: Dict | None = None, timeout: int = 15, retries: int = 3) -> Dict:
    backoff = 1.0
    last_err = None
    
    for attempt in range(retries):
        try:
            timeout_obj = aiohttp.ClientTimeout(total=timeout, connect=10)
            if method.upper() == "GET":
                async with session.get(url, timeout=timeout_obj) as resp:
                    if resp.status != 200:
                        text = await resp.text()
                        last_err = f"HTTP {resp.status}: {text[:500]}"
                    else:
                        return await resp.json()
            else:
                async with session.post(url, json=json_payload, timeout=timeout_obj) as resp:
                    if resp.status != 200:
                        text = await resp.text()
                        last_err = f"HTTP {resp.status}: {text[:500]}"
                    else:
                        return await resp.json()
        except (aiohttp.ClientConnectorError, aiohttp.ClientConnectorDNSError) as e:
            last_err = f"Connection error: {str(e)}"
            logger.warning(f"[HTTP_RETRY] attempt={attempt+1}/{retries} err={last_err}")
        except asyncio.TimeoutError as e:
            last_err = f"Timeout after {timeout}s"
            logger.warning(f"[HTTP_RETRY] attempt={attempt+1}/{retries} err={last_err}")
        except Exception as e:
            last_err = f"{type(e).__name__}: {str(e)}"
            logger.warning(f"[HTTP_RETRY] attempt={attempt+1}/{retries} err={last_err}")

        if attempt < retries - 1:
            await asyncio.sleep(backoff)
            backoff *= 2

    return {"_error": "request_failed", "_detail": last_err or "unknown_error"}


def main():
    concurrency = 1  # Start with 1 to debug
    dry_run = False

    try:
        results = asyncio.run(runner(concurrency=concurrency, dry_run=dry_run))
        if results:
            ok = sum(1 for r in results if r.get("status") == "success")
            logger.info(f"\n{'='*60}")
            logger.info(f"COMPLETED: {ok}/{len(results)} successful")
            logger.info(f"{'='*60}\n")
            
            for r in results:
                status_icon = "✓" if r.get("status") == "success" else "✗"
                logger.info(
                    f"{status_icon} {str(r.get('mint'))[:8]}... | "
                    f"status={r.get('status')} | "
                    f"sig={r.get('signature', 'N/A')[:8]}... | "
                    f"err={r.get('error', '')}"
                )
    except KeyboardInterrupt:
        logger.warning("Interrupted by user.")
    except Exception as e:
        import traceback
        logger.error(f"Fatal error: {e}")
        logger.error(traceback.format_exc())


if __name__ == "__main__":
    main()
