# etrade_consumer.py
import os
import json
from typing import List
import time as pyTime
from datetime import datetime, timezone
from cryptography.fernet import Fernet
from requests_oauthlib import OAuth1Session
from urllib.parse import urlencode
from models.generated.Account import Account, PortfolioAccount
from models.generated.Position import Position
from models.option import OptionContract,Product,Quick,OptionGreeks,ProductId
from services.threading.api_worker import ApiWorker,HttpMethod
from services.logging.logger_singleton import getLogger
from services.token_status import TokenStatus
from services.utils import write_scratch
import enum

TOKEN_LIFETIME_DAYS = 90

class ActionResponse(enum.Enum):
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    RESTART = "RESTART"
    QUIT = "QUIT"


class TokenExpiredError(Exception):
    """Raised when OAuth token is invalid or expired."""
    pass

class NoOptionsError(Exception):
    """Raised when a ticker has no options."""
    pass

class NoExpiryError(Exception):
    """Raised when ticker has no expiry dates"""
    pass

class InvalidSymbolError(Exception):
    """Raised when a ticker has no options."""
    pass


class EtradeConsumer:
    def __init__(self, apiWorker: ApiWorker = None, sandbox=False, debug=False):
        self.debug = debug
        self.sandbox = sandbox
        self.apiWorker = apiWorker
        self.token_status = TokenStatus()
        self.logger = getLogger()
        envType = "nonProd" if sandbox else "prod"

        self.consumer_key, self.consumer_secret = self.load_encrypted_etrade_keysecret(sandbox)
        self.token_file = os.path.join("encryption", f"etrade_tokens_{envType}.json")
        self.base_url = "https://apisb.etrade.com" if sandbox else "https://api.etrade.com"

        if not self.consumer_key:
            raise Exception("Missing E*TRADE consumer key")

        if not os.path.exists(self.token_file):
            self.logger.logMessage("No token file found. Starting OAuth...")
            while True:
                generate_status = self.generate_token()
                if generate_status in {ActionResponse.FAILURE, ActionResponse.QUIT} :
                    raise Exception("Failed to generate access token.")
                elif generate_status == ActionResponse.RESTART:
                    continue
                elif generate_status == ActionResponse.SUCCESS:
                    break
        else:
            self.load_tokens()


    def get(self, url: str, headers=None, params=None):
        # Existing headers (may be None)
        headers = headers or {}  # ensure it's a dict

        # Add/overwrite Accept header
        headers.update({"Accept": "application/json"})
        
        if self.apiWorker is not None:
            error = ""
            r = self.apiWorker.call_api(HttpMethod.GET, url, headers=headers, params=params)
            if r is not None:
                if r.ok:
                    if hasattr(r,"response"):
                        return r.response
                    else:
                        raise Exception(f"Response to {url} does not contain a response attribute: {json.dumps(r, indent=2, default=str)}")
                else:
                    # --- detect HTTP 401 Unauthorized ---
                    status_code = None
                    if r.status_code is not None:
                        status_code = r.status_code
                    else:
                        if hasattr(r,"response"):
                            status_code = r.response.status_code
                    if status_code == 401:
                        self.logger.logMessage("[Auth] Token expired or unauthorized, need to regenerate")
                        self.token_status.set_status(False)
                        raise TokenExpiredError("OAuth token expired")  
                    elif status_code == 408:
                        raise TimeoutError    
                    elif status_code == 400:
                        error = ""
                        if hasattr(r,"response"):
                            error = r.response.text
                        else:
                            error = r.error
                        if "10033" in error:
                            raise InvalidSymbolError(error)
                        elif "10031" in error or "10032 in error":
                            write_scratch(f"Error: {error} | Params: {str(params)}")
                            raise (NoOptionsError(error))
                        else:
                            write_scratch(f"Error: {error} | Params: {str(params)}")
                            raise Exception(error)
   
                    else:
                        raise Exception(f"Response received an error. Calculated status code: {status_code}. Response: {json.dumps(r, indent=2, default=str)}")        
            else:
                raise Exception(f"No response received for {url}")
        else:
            try:
                return self.session.get(url, headers=headers, params=params)
            
            except Exception as e:
                error = f"[GET Exception] {e} for URL: {url}"
                self.logger.logMessage(error)
                return None,error




    def put(self, url, headers=None, params=None, data=None):
        """
        Send a PUT request with optional headers, query params, and JSON body.
        
        :param url: endpoint URL
        :param headers: dict of headers
        :param params: dict of query parameters
        :param data: dict/json payload to send in the body
        """
                
        headers = headers or {}
        headers.update({
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.oauth_token}"
        })
        
        if self.apiWorker is not None:
            # apiWorker expects params; encode data as JSON string
            payload = json.dumps(data) if data else None
            response = self.apiWorker.call_api(HttpMethod.PUT, url, headers=headers, params=params, data=payload)
            if response.get("ok"):
                return response.get("data")
            else:
                self.logger.logMessage(f"Error {response.get('status_code')}: {response.get('error')}")
                return None
        else:
            try:
                return self.session.put(url, headers=headers, params=params, json=data)
            except Exception as e:
                self.logger.logMessage(f"[PUT Exception] {e} for URL: {url}")
                return None


    def post(self, url, headers=None, params=None, data=None):
        """
        Send a POST request with optional headers, query params, and JSON body.

        :param url: endpoint URL
        :param headers: dict of headers
        :param params: dict of query parameters
        :param data: dict/json payload to send in the body
        """
        headers = headers or {}
        headers.update({
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.oauth_token}"
        })

        if self.apiWorker is not None:
            # apiWorker expects params; encode data as JSON string
            payload = json.dumps(data) if data else None
            response = self.apiWorker.call_api(HttpMethod.POST, url, headers=headers, params=params, data=payload)
            if response.get("ok"):
                return response.get("data")
            else:
                self.logger.logMessage(f"Error {response.get('status_code')}: {response.get('error')}")
                return None
        else:
            try:
                resp = self.session.post(url, headers=headers, params=params, json=data)
                return resp
            except Exception as e:
                self.logger.logMessage(f"[POST Exception] {e} for URL: {url}")
                return None


    # ------------------- TOKENS -------------------
    def load_tokens(self, generate_new_token=True):
        """Load saved tokens or generate if missing/expired."""
        token_data = {}
        if os.path.exists(self.token_file):
            with open(self.token_file, "r") as f:
                token_data = json.load(f)

        self.oauth_token = token_data.get("oauth_token")
        self.oauth_token_secret = token_data.get("oauth_token_secret")
        created_at = token_data.get("created_at", 0)

        # Build OAuth1 session if we have tokens
        if self.oauth_token and self.oauth_token_secret:
            self.session = OAuth1Session(
                self.consumer_key,
                client_secret=self.consumer_secret,
                resource_owner_key=self.oauth_token,
                resource_owner_secret=self.oauth_token_secret,
            )

        # Check token age
        local_tz = datetime.now().astimezone().tzinfo
        token_age_days = (datetime.now().astimezone() - datetime.fromtimestamp(created_at, tz=local_tz)).days
        if (not self.oauth_token or token_age_days >= TOKEN_LIFETIME_DAYS) and generate_new_token:
            self.logger.logMessage(f"Token missing or expired (age={token_age_days}d). Generating new token...")
            if not self.generate_token():
                raise Exception("Failed to generate new OAuth token.")
        else:
            # Extra check: make sure the token actually works with the API
            if not self._check_session_valid():
                if generate_new_token:
                    self.logger.logMessage("Token invalid according to API. Generating new token...")
                    if not self.generate_token():
                        raise Exception("Failed to generate new OAuth token.")
                else:
                    self.logger.logMessage("Existing token invalid but not set up to generate new token")
            else:
                self.logger.logMessage("Loaded token valid")



    def _validate_tokens(self):
        """
        Validate current tokens; attempt refresh first.
        If refresh fails, fallback to full manual token generation.
        """
        try:
            if self._check_session_valid():
                self.token_status.set_status(True)
                return True
            return self.generate_token()
        except Exception as e:
            self.token_status.set_status(False)
            self.logger.logMessage(f"[Token Validation] Exception: {e}")
            return False

    def _check_session_valid(self):
        """Simple API test to check if the current session is valid."""
        try:
            url = f"{self.base_url}/v1/accounts/list.json"
            r = self.get(url)
            return r and getattr(r, "status_code", 200) == 200
        except Exception as e:
            self.logger.logMessage(f"[Token Validation] Session check failed: {e}")
            return False

    def generate_token(self) -> ActionResponse:
        """
        Interactive OAuth flow for E*TRADE with retry logic.
        User can retry PIN, regenerate a new token/URL, or exit.
        """
        while True:
            # Step 1: Get request token
            try:
                request_token_url = f"{self.base_url}/oauth/request_token"

                oauth = OAuth1Session(
                    self.consumer_key,
                    client_secret=self.consumer_secret,
                    callback_uri="oob"
                )
                fetch_response = oauth.fetch_request_token(request_token_url)
                resource_owner_key = fetch_response.get("oauth_token")
                resource_owner_secret = fetch_response.get("oauth_token_secret")
            except Exception as e:
                self.logger.logMessage(f"[Auth] Failed to obtain request token: {e}")
                return ActionResponse.FAILURE

            # Step 2: Provide user the authorization URL            
            authorize_base = "https://us.etrade.com/e/t/etws/authorize"
            params = {"key": self.consumer_key, "token": resource_owner_key}
            authorization_url = f"{authorize_base}?{urlencode(params)}"
            self.logger.logMessage(f"[Auth] Please go to this URL and authorize: {authorization_url}")

            # Step 3: Prompt for PIN until success, restart, or exit
            while True:
                verifier = input(
                    "[Auth] Enter the 7-digit PIN "
                    "(or type 'restart' for new URL, 'exit' to cancel): "
                ).strip()

                if verifier.lower() == "exit":
                    self.logger.logMessage("[Auth] User aborted token generation")
                    return ActionResponse.QUIT
                if verifier.lower() == "restart":
                    self.logger.logMessage("[Auth] Restarting OAuth flow with new request token")
                    return ActionResponse.RESTART  # break inner loop, restart outer flow

                access_token_url = f"{self.base_url}/oauth/access_token"

                try:
                    # Step 4: Exchange PIN for access token
                    oauth = OAuth1Session(
                        self.consumer_key,
                        client_secret=self.consumer_secret,
                        resource_owner_key=resource_owner_key,
                        resource_owner_secret=resource_owner_secret,
                        verifier=verifier
                    )
                    access_token_response = oauth.fetch_access_token(access_token_url)

                    # Store tokens
                    self.oauth_token = access_token_response.get("oauth_token")
                    self.oauth_token_secret = access_token_response.get("oauth_token_secret")                    # Persist to disk
                    self.session = OAuth1Session(
                        self.consumer_key,
                        client_secret=self.consumer_secret,
                        resource_owner_key=self.oauth_token,
                        resource_owner_secret=self.oauth_token_secret,
                    )                    
                    self.save_tokens()

                    self.logger.logMessage("[Auth] Access token successfully obtained and saved")
                    self.token_status.set_status(True)
                    return ActionResponse.SUCCESS
                except Exception as e:
                    self.token_status.set_status(False)
                    self.logger.logMessage(f"[Auth] Invalid PIN or error during exchange: {e}")
                    self.logger.logMessage("[Auth] Try again, type 'restart' for new URL, or 'exit'.")
                    continue  # loop again for new PIN


    def save_tokens(self):
        """Save the current token data to disk with a timestamp."""
        with open(self.token_file, "w") as f:
            json.dump({
                "oauth_token": self.oauth_token,
                "oauth_token_secret": self.oauth_token_secret,
                "created_at": int(pyTime.time())  # store as epoch
            }, f)
        self.token_status.set_status(True)

    # ------------------- HELPERS -------------------
    def get_headers(self):
        return {"Content-Type": "application/json"}

    def load_encrypted_etrade_keysecret(self, sandbox=True):
        with open("encryption/secret.key", "rb") as key_file:
            key = key_file.read()
        sandbox_suffix = "sandbox" if sandbox else "prod"
        with open(f"encryption/etrade_consumer_key_{sandbox_suffix}.enc", "rb") as enc_file:
            encrypted_key = enc_file.read()
        with open(f"encryption/etrade_consumer_secret_{sandbox_suffix}.enc", "rb") as enc_file:
            encrypted_secret = enc_file.read()
        f = Fernet(key)
        return f.decrypt(encrypted_key).decode(), f.decrypt(encrypted_secret).decode()

    # ------------------- ACCOUNT / PORTFOLIO -------------------
    def get_accounts(self):
        url = f"{self.base_url}/v1/accounts/list.json"
        r = self.get(url)
        try:
            accts = r.json().get("AccountListResponse", {}).get("Accounts", {}).get("Account", [])
            return [Account(**acct) for acct in accts]
        except Exception as e:
            self.logger.logMessage(f"[ERROR] Failed to parse account ID: {e}")
            return []

    def get_positions(self):
        accounts = self.get_accounts()
        all_positions = []
        for acct in accounts:
            url = f"{self.base_url}/v1/accounts/{acct.accountIdKey}/portfolio.json"
            r = self.get(url)
            data = r.json()
            account_portfolios = data.get("PortfolioResponse", {}).get("AccountPortfolio", [])
            for acct_raw in account_portfolios:
                portfolio = PortfolioAccount.from_dict(acct_raw)
                all_positions.extend(portfolio.Position or [])
        return all_positions
    
        #How much capital is currently outstanding (ie don't buy more than comfortable)
    def get_open_exposure(self):
        positions = self.get_positions()
        if positions is not None:
            return sum(p.totalCost for p in positions)
        return None

    # ------------------- OPTION CHAINS -------------------
    
    def get_expiry_dates(self, symbol):
        url = f"{self.base_url}/v1/market/optionexpiredate.json"
        params = {"symbol": symbol}
        try:
            response = self.get(url, params=params)
            
            #before we implement, make sure can have unique handling depending on caller
            #self.inspect_response(symbol,response)
            
        except Exception as e:
            data = f"Ticker: {symbol}, Params: {params}"
            self.handle_exception(e,data)
            
        if response.status_code == 204:
            raise NoExpiryError(f"Ticker returned no expiry dates")

        try:
            data = response.json()
            expiry_list = data.get("OptionExpireDateResponse", {}).get("ExpirationDate", [])
            # Return simplified dicts with year/month/day
            return [
                {"year": e.get("year"), "month": e.get("month"), "day": e.get("day"), "expiryType": e.get("expiryType")}
                for e in expiry_list
            ]
        except Exception as e:
            raise Exception(f"Failed to parse expiry dates response for {symbol}: {e}")


    def get_option_chain(self, symbol, date_range=None):
        url = f"{self.base_url}/v1/market/optionchains.json"
        params = {
            "symbol": symbol,
            "includeWeekly": "true",
            "strategy": "SINGLE",
            "skipAdjusted": "false",
            "chainType": "CALL",
        }

        # Resolve expiry dates
        expiry_dates = []
        if date_range is None:
            # Default: get all expiries
            expiry_dates = self.get_expiry_dates(symbol=symbol)
        elif "year" in date_range and "month" in date_range:
            # Single expiry (shortcut, no need to fetch all)
            expiry_dates = [date_range]
        elif "start" in date_range and "end" in date_range:
            # Range → fetch all, then filter
            all_expiries = self.get_expiry_dates(symbol=symbol)
            start_y, start_m = date_range["start"]["year"], date_range["start"]["month"]
            end_y, end_m = date_range["end"]["year"], date_range["end"]["month"]

            def in_range(exp):
                y, m = exp["year"], exp["month"]
                return (y > start_y or (y == start_y and m >= start_m)) and \
                    (y < end_y or (y == end_y and m <= end_m))

            expiry_dates = [exp for exp in all_expiries if in_range(exp)]
        else:
            raise ValueError("date_range must be either {year, month} or {start:{}, end:{}}")

        results = []

        # Loop across all target expiries
        for expiry in expiry_dates:
            params.update({
                "expiryYear": expiry["year"],
                "expiryMonth": expiry["month"],
                "expiryDay": expiry["day"]
            })

            try:
                response = self.get(url, params=params)
                self.inspect_response(symbol, response)
            except Exception as e:
                data = f"Ticker: {symbol}, Params: {str(params)}"
                self.handle_exception(e,data)

            # If we are here means response.ok == true
            local_tz = datetime.now().astimezone().tzinfo

            try:
                chain_data = response.json().get("OptionChainResponse", {})
                near_price = chain_data.get("nearPrice")
                expiry_dict = chain_data.get("SelectedED", {})
                expiry_date = datetime(
                    year=expiry_dict.get("year", 1970),
                    month=expiry_dict.get("month", 1),
                    day=expiry_dict.get("day", 1),
                    tzinfo=local_tz
                )

                for optionPair in chain_data.get("OptionPair", []):
                    call = optionPair.get("Call", {})
                    call["expiryDate"] = expiry_date
                    call["nearPrice"] = near_price
                    call_greeks = call.get("OptionGreeks", {})
                    option_greeks = OptionGreeks(**call_greeks)

                    product = Product(
                        symbol=call.get("symbol"),
                        securityType=call.get("optionType"),
                        callPut="CALL" if call.get("optionType") == "CALL" else "PUT",
                        strikePrice=call.get("strikePrice"),
                        productId=ProductId(symbol=call.get("symbol"), typeCode=call.get("optionType")),
                        expiryDay=expiry_date.day,
                        expiryMonth=expiry_date.month,
                        expiryYear=expiry_date.year
                    )

                    quick = Quick(
                        lastTrade=call.get("lastPrice"),
                        lastTradeTime=None,
                        change=None,
                        changePct=None,
                        volume=call.get("volume"),
                        quoteStatus=None
                    )

                    option_fields = {k: call[k] for k in [
                        "symbol", "optionType", "strikePrice", "displaySymbol", "osiKey",
                        "bid", "ask", "bidSize", "askSize", "inTheMoney", "volume",
                        "openInterest", "netChange", "lastPrice", "quoteDetail",
                        "optionCategory", "timeStamp", "adjustedFlag", "expiryDate", "nearPrice"
                    ] if k in call}

                    option = OptionContract(
                        **option_fields,
                        OptionGreeks=option_greeks,
                        quick=quick,
                        product=product
                    )

                    results.append(option)
            except Exception as e:
                errorMessage = f"[ERROR] Failed to parse option chain for {symbol}: {e}"
                raise Exception(errorMessage)

        return results

    # ------------------- QUOTES -------------------
    def get_quote(self, symbol):
        url = f"{self.base_url}/v1/market/quote/{symbol}.json"
        r,error= self.get(url)
        try:
            qdata = r.json().get("QuoteResponse", {}).get("QuoteData", [])[0]
            product = Product(symbol=symbol)
            quick = Quick(
                lastTrade=qdata.get("lastTrade"),
                lastTradeTime=None,
                change=qdata.get("change"),
                changePct=qdata.get("changePct"),
                volume=qdata.get("volume"),
                quoteStatus=qdata.get("quoteStatus")
            )
            return Position(Product=product, Quick=quick)
        except Exception as e:
            self.logger.logMessage(f"[ERROR] Failed to parse quote for {symbol}: {e}")
            return None
        
    def handle_exception(self,e, data):
        if isinstance(e,NoOptionsError):
            write_scratch(f"Exception: {str(e)} | Data: {data}")
        raise e
    
    def inspect_response(self,symbol, response):
        if response is None:
            raise Exception("No Response info was received")
        elif not response.ok:
            if response.status_code == 400:
                raise NoOptionsError(f"No Options received for {symbol}")
            elif response.status_code == 408:
                raise TimeoutError(f"Timeout received when processing options for {symbol}")

            try:
                error = json.loads(response.text)
                error_code = error["Error"]["code"]

                if error_code == 10033 or "10033" in error:
                    raise InvalidSymbolError(f"Invalid symbol for {symbol}")
                elif error_code in (10031, 10032) or "10031" in error or "10032" in error:
                    raise NoOptionsError(f"No Options available for ticker: {symbol}")
                else:
                    raise Exception(error)
            except Exception as e:
                raise Exception(f"Error parsing response error for ticker {symbol}: {e}") 


# ------------------- FORCE TOKEN GENERATION (OUTSIDE CLASS) -------------------
def force_generate_new_token(sandbox=False):
    consumer = EtradeConsumer(sandbox=sandbox)
    return

