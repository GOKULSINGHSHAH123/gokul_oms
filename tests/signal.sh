# CORRECT SIGNAL
python3 send_signal.py \
--exchangeSegment MCXFO \
--exchangeInstrumentID 486502 \
--orderType LIMIT \
--orderSide BUY \
--orderQuantity 150 \
--limitPrice 9100 \
--algoName TestAlgo


#Token not found for client .\
# --exchangeSegment MCXFO \
# --exchangeInstrumentID 486502 \
# --orderType LIMIT \
# --orderSide BUY \
# --orderQuantity 100 \
# --limitPrice 9100 \
# --algoName TestAlgo


# # WRONG SIGNAL WITH INCORRECT EXCHANGE SEGMENT
# python3 send_signal.py \
# --exchangeSegment BSEFO \
# --exchangeInstrumentID 59460 \
# --orderType LIMIT \
# --orderSide BUY \
# --orderQuantity 500 \
# --limitPrice 1424.2 \
# --algoName TestAlgo

# # WRONG SIGNAL WITH INCORRECT EXCHANGE INSTRUMENT ID
# python3 send_signal.py \
# --exchangeSegment NSEFO \
# --exchangeInstrumentID 1234 \
# --orderType LIMIT \
# --orderSide BUY \
# --orderQuantity 500 \
# --limitPrice 1424.2 \
# --algoName TestAlgo

# # WRONG SIGNAL WITH INCORRECT ORDER TYPE
# python3 send_signal.py \
# --exchangeSegment NSEFO \
# --exchangeInstrumentID 59460 \
# --orderType RANDOM \
# --orderSide BUY \
# --orderQuantity 500 \
# --limitPrice 1424.2 \
# --algoName TestAlgo

# # WRONG SIGNAL WITH INCORRECT ORDER SIDE
# python3 send_signal.py \
# --exchangeSegment NSEFO \
# --exchangeInstrumentID 59460 \
# --orderType LIMIT \
# --orderSide HOLD \
# --orderQuantity 500 \
# --limitPrice 1424.2 \
# --algoName TestAlgo

# # WRONG SIGNAL WITH INCORRECT ORDER QUANTITY
# python3 send_signal.py \
# --exchangeSegment NSEFO \
# --exchangeInstrumentID 59460 \
# --orderType LIMIT \
# --orderSide BUY \
# --orderQuantity 500 + 100 \
# --limitPrice 1424.2 \
# --algoName TestAlgo

# # WRONG SIGNAL WITH INCORRECT LIMIT PRICE
# python3 send_signal.py \
# --exchangeSegment NSEFO \
# --exchangeInstrumentID 59460 \
# --orderType LIMIT \
# --orderSide BUY \
# --orderQuantity 500 + 100 \
# --limitPrice 1424.2 + 10000 \
# --algoName TestAlgo

# # WRONG SIGNAL WITH INCORRECT ALGO NAME
# python3 send_signal.py \
# --exchangeSegment NSEFO \
# --exchangeInstrumentID 59460 \
# --orderType LIMIT \
# --orderSide BUY \
# --orderQuantity 500 + 100 \
# --limitPrice 1424.2 \
# --algoName TestAlgo_RANDOM