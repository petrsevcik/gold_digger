import yfinance as yf

dat = yf.Ticker("WBD")
#print(dat.options) # it gives dates tuple ('2025-07-25', '2025-08-01', '2025-08-08', '2025-08-15', '2025-08-22', '2025-08-29', '2025-09-19', '2025-10-17', '2025-11-21', '2025-12-19', '2026-01-16', '2026-03-20', '2026-06-18', '2026-09-18', '2026-12-18', '2027-01-15')
option_date = dat.options
for _date in option_date:
    call_op = dat.option_chain(_date).puts
    print(call_op)
