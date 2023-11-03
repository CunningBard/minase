# Minase query language
## Introduction

## Operations
### Select
```
select [where <condition: expr>] from <table: number> <column: number>
select table <table: number>
```

### Insert
```
insert <values: expr+> into <table: number>
```

### Update
```
update <table: number> <column: number> with <values: expr+> where <condition: expr>
```

### Delete
```
delete [where <condition: expr>] from <table: number> <column: number>
```