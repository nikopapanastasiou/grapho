# grapho
[![Go](https://github.com/nikopapanastasiou/grapho/actions/workflows/go.yml/badge.svg)](https://github.com/nikopapanastasiou/grapho/actions/workflows/go.yml)

In-memory graph DB written in Go.

## Playing around

Starting the server:
```bash
cd server
go run cmd/server
```

Connecting to the server:
```bash
cd client
go run cmd/client
```

Running client commands:
```bash
CREATE NODE Person (name: string, age: int);
CREATE NODE Place (name: string);
CREATE EDGE Knows (FROM Person ONE, TO Person MANY);
CREATE EDGE LivesIn (FROM Person ONE, TO Place ONE);

INSERT NODE Person (name: "John", age: 30);
INSERT NODE Person (name: "Jane", age: 25);
INSERT NODE Place (name: "New York");
INSERT NODE Place (name: "Los Angeles");

MATCH PERSON WHERE name: "John";
```