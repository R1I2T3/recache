
set -e 

(
  cd "$(dirname "$0")" 
  go build -o /tmp/redis-go app/*.go
)

exec /tmp/redis-go "$@"
