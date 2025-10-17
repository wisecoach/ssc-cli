for key_file in $(find /mnt/E/gowork/src/github.com/harmony-one/harmony/.hmy/expr_accounts -maxdepth 1);
do
  ls $key_file
  nohup conda run -n cli-py python transfer_per_account.py $key_file &
done