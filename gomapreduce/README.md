# gomapreduce

Minimal Go workspace demonstrating a tiny Map/Reduce-style package.

Structure:

- `main.go` — example runner that prints word counts.
- `mapreduce/` — package with `Map` and `Reduce` functions and tests.

How to run:

```bash
cd /home/alexrdp/Documents/MapReduce/gomapreduce
go mod init gomapreduce   # runs once to create go.mod
go test ./...             # run tests
go run main.go            # run example
```
