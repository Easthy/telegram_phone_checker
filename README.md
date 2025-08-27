### To run 
```
python3 telegram_phone_checker.py
```
  
The number of batchs to check could be set at config.yaml. Stats of numbers checked by accounts saves to `account_limits.json`. Last checked batcn number saves to `batch_state.yaml`
  
--------------------- OLD --------------------------
### To run 
```
python3 telegram_phone_checker.py group_1_all.csv telegram_check_results.csv 10 51 52
```
group_1_all.csv - csv file with only 1 column - phone numbers in international format as `799955512344`
telegram_check_results.csv - file where to store check results  
10 - batch size to check with 1 account  
51 - starting offset of batch  
52 - end batch number  
