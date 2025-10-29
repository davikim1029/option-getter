import os
import json
import webbrowser
import time as pyTime
from datetime import datetime, timezone
from cryptography.fernet import Fernet
from requests_oauthlib import OAuth1Session
from urllib.parse import urlencode
from models.generated.Account import Account, PortfolioAccount
from models.generated.Position import Position
from models.option import OptionContract,Product,Quick,OptionGreeks,ProductId

TOKEN_LIFETIME_DAYS = 90


class EtradeConsumerLite:
    def __init__(self, sandbox=False, debug=False):
        self.debug = debug
        self.sandbox = sandbox        
        envType = "nonProd" if sandbox else "prod"

        self.consumer_key, self.consumer_secret = self.load_encrypted_etrade_keysecret(sandbox)
        self.token_file = os.path.join("encryption", f"etrade_tokens_{envType}.json")
        self.base_url = "https://apisb.etrade.com" if sandbox else "https://api.etrade.com"

        if not self.consumer_key:
            raise Exception("Missing E*TRADE consumer key")

        if not os.path.exists(self.token_file):
            print("No token file found. Starting OAuth...")
            if not self.generate_token():
                raise Exception("Failed to generate access token.")
        else:
            self.load_tokens()


    def get(self, url: str, headers=None, params=None):
        # Existing headers (may be None)
        headers = headers or {}  # ensure it's a dict

        # Add/overwrite Accept header
        headers.update({"Accept": "application/json"})
        
        try:
            return self.session.get(url, headers=headers, params=params)
        
        except Exception as e:
            error = f"[GET Exception] {e} for URL: {url}"
            print(error)
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
            print(f"Token missing or expired (age={token_age_days}d). Generating new token...")
            if not self.generate_token():
                raise Exception("Failed to generate new OAuth token.")
        else:
            # Extra check: make sure the token actually works with the API
            if not self._check_session_valid():
                if generate_new_token:
                    print("Token invalid according to API. Generating new token...")
                    if not self.generate_token():
                        raise Exception("Failed to generate new OAuth token.")
                else:
                    print("Existing token invalid but not set up to generate new token")
            else:
                print("Loaded token valid")



    def _validate_tokens(self):
        """
        Validate current tokens; attempt refresh first.
        If refresh fails, fallback to full manual token generation.
        """
        try:
            if self._check_session_valid():
                return True
            return self.generate_token()
        except Exception as e:
            print(f"[Token Validation] Exception: {e}")
            return False

    def _check_session_valid(self):
        """Simple API test to check if the current session is valid."""
        try:
            url = f"{self.base_url}/v1/accounts/list.json"
            r = self.get(url)
            return r and getattr(r, "status_code", 200) == 200
        except Exception as e:
            print(f"[Token Validation] Session check failed: {e}")
            return False

    def generate_token(self):
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
                print(f"[Auth] Failed to obtain request token: {e}")
                return

            # Step 2: Provide user the authorization URL            
            authorize_base = "https://us.etrade.com/e/t/etws/authorize"
            params = {"key": self.consumer_key, "token": resource_owner_key}
            authorization_url = f"{authorize_base}?{urlencode(params)}"
            print(f"[Auth] Please go to this URL and authorize: {authorization_url}")

            # Step 3: Prompt for PIN until success, restart, or exit
            while True:
                verifier = input(
                    "[Auth] Enter the 7-digit PIN "
                    "(or type 'restart' for new URL, 'exit' to cancel): "
                ).strip()

                if verifier.lower() == "exit":
                    print("[Auth] User aborted token generation")
                    return False
                if verifier.lower() == "restart":
                    print("[Auth] Restarting OAuth flow with new request token")
                    break  # break inner loop, restart outer flow

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

                    print("[Auth] Access token successfully obtained and saved")
                    return True
                except Exception as e:
                    print(f"[Auth] Invalid PIN or error during exchange: {e}")
                    print("[Auth] Try again, type 'restart' for new URL, or 'exit'.")
                    continue  # loop again for new PIN
            
            #We shouldn't hit this so trigger as failure
            return False


    def save_tokens(self):
        """Save the current token data to disk with a timestamp."""
        with open(self.token_file, "w") as f:
            json.dump({
                "oauth_token": self.oauth_token,
                "oauth_token_secret": self.oauth_token_secret,
                "created_at": int(pyTime.time())  # store as epoch
            }, f)

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
            print(f"[ERROR] Failed to parse account ID: {e}")
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
        except Exception as e:
            raise Exception(f"Failed to fetch expiry dates: {str(e)}")
        
        if response is None:
            return None

        try:
            return response.json()
        except:
            return str(response)
        
        
        
    def get_option_chain(self, symbol):
        url = f"{self.base_url}/v1/market/optionchains.json"
        params = {
            "symbol": symbol,
            "includeWeekly": "true",
            "strategy": "SINGLE",
            "skipAdjusted": "false",
            "chainType": "CALL",
        }
        
        month,year,day = get_valid_month_year_day()
        if month is not None:
            params.update({"expiryMonth": month})
        if year is not None:
            params.update({
                    "expiryYear": year,
            })
        if day is not None:
            params.update({"expiryDay":day})
        r = self.get(url, params=params)
        if r is None:
            return None

        try:
            return r.json()
        except Exception as e:
            print(f"[ERROR] Failed to parse option chain for {symbol}: {str(e)}")
            return None

    # ------------------- QUOTES -------------------
    def get_quote(self, symbol):
        url = f"{self.base_url}/v1/market/quote/{symbol}.json"
        r= self.get(url)
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
            print(f"[ERROR] Failed to parse quote for {symbol}: {e}")
            return None


# ------------------- Helpers -------------------- #
import calendar

def get_valid_month_year_day():
    # Month
    while True:
        month_str = input("Enter month (1-12, or leave blank for None): ").strip()
        if not month_str:  # empty input
            month = None
            break
        try:
            month = int(month_str)
            if 1 <= month <= 12:
                break
            else:
                print("Invalid month. Please enter 1–12, or leave blank for None.")
        except ValueError:
            print("Invalid input. Please enter a number or leave blank.")

    # Year
    while True:
        year_str = input("Enter year (e.g., 2025, or leave blank for None): ").strip()
        if not year_str:
            year = None
            break
        try:
            year = int(year_str)
            if year > 0:
                break
            else:
                print("Year must be positive, or leave blank for None.")
        except ValueError:
            print("Invalid input. Please enter a valid year or leave blank.")

    # Day (only if month and year provided)
    day = None
    if month is not None and year is not None:
        _, max_day = calendar.monthrange(year, month)  # (weekday, number_of_days)
        while True:
            day_str = input(f"Enter day (1-{max_day}, or leave blank for None): ").strip()
            if not day_str:
                day = None
                break
            try:
                day = int(day_str)
                if 1 <= day <= max_day:
                    break
                else:
                    print(f"Invalid day. Please enter 1–{max_day}, or leave blank for None.")
            except ValueError:
                print("Invalid input. Please enter a number or leave blank.")

    return year, month, day

