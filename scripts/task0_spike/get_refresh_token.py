#!/usr/bin/env python3
"""
One-time helper to generate a TastyTrade OAuth2 refresh token.

Only needed if you can't use the "Create Grant" button under
OAuth Applications > Manage on TastyTrade's website (that's the simpler,
no-code route - try that first). This script is the only option for
sandbox accounts.

This opens a browser window, has you paste your client_id and
client_secret, and walks you through TastyTrade's consent screen. At the
end it prints a refresh_token - save that into your .env file as
TASTYTRADE_REFRESH_TOKEN (along with TASTYTRADE_CLIENT_SECRET, which you
already have).

Usage:
    python get_refresh_token.py            # production
    python get_refresh_token.py --sandbox  # sandbox/cert account
"""
import argparse

from tastytrade.oauth import login

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sandbox", action="store_true")
    args = parser.parse_args()

    print("A browser window will open. Paste your client_id and client_secret")
    print("when prompted, then follow the consent flow.\n")
    login(is_test=args.sandbox)
    print("\nDone - copy the refresh_token printed above into your .env file")
    print("as TASTYTRADE_REFRESH_TOKEN.")
