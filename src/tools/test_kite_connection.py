# In the main function, update the strike price test section:

# 3. Test getting instruments
if expiry_dates:
    # Calculate strikes around the current price
    nifty_price = 0
    try:
        quotes = kite_provider.get_quote([("NSE", "NIFTY 50")])
        key = next(iter(quotes))
        nifty_price = quotes[key].get("last_price", 0)
    except:
        nifty_price = 24600  # Fallback if quote fails
    
    # Generate strikes around the current price (with 50 point intervals)
    base_strike = round(nifty_price / 50) * 50
    strikes = [base_strike - 100, base_strike - 50, base_strike, base_strike + 50, base_strike + 100]
    
    logger.info(f"Testing option instruments for NIFTY, {expiry_dates[0]}, strikes {strikes}")
    instruments = kite_provider.get_option_instruments("NIFTY", expiry_dates[0], strikes)
    if instruments:
        logger.info(f"✅ Got {len(instruments)} option instruments")
        # Print first instrument
        if instruments:
            logger.info(f"Sample: {instruments[0].get('tradingsymbol')}")
    else:
        logger.warning("No option instruments returned")