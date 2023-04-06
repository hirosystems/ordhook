
## How to run

In a console, we will launch `vault-monitor`. `vault-monitor` is a program that will be processing the events triggered by `chainhook`. Ruby on Rails (ruby 2.7+, rails 7+) was used to demonstrate that Chainhooks is a language agnostic layer. 

```bash
# Navigate to vault monitor directory
$ cd vault-monitor

# Install dependencies
$ bundle install

# Create database and run db migrations (will use sqlite in development mode)
$ rails db:migrate

# Run program
$ rails server
```

`vault-monitor` exposes an admin readonly user interface at this address `http://localhost:3000/admin`.

In another console, start replaying events using the command:

```bash
$ chainhook predicates scan ./arkadiko.json --mainnet
```
