cd /home/alexrdp/Documents/MapReduce/mit/6.5840/src/mrapps/
go build -buildmode=plugin /home/alexrdp/Documents/MapReduce/mit/6.5840/src/mrapps/wc.go
cd /home/alexrdp/Documents/MapReduce/mit/6.5840/src/main/
go run /home/alexrdp/Documents/MapReduce/mit/6.5840/src/main/mrworker.go /home/alexrdp/Documents/MapReduce/mit/6.5840/src/mrapps/wc.so