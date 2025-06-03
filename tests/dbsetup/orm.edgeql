insert User {name := 'Alice'};
insert User {name := 'Billie'};
insert User {name := 'Cameron'};
insert User {name := 'Dana'};
insert User {name := 'Elsa'};
insert User {name := 'Zoe'};

with U := User
update User
filter .name = 'Alice'
set {
    friends := assert_distinct((
        for t in {('Billie', 1), ('Zoe', 100)}
        select U {@weight := t.1} filter .name = t.0
    ))
};
with U := User
update User
filter .name = 'Billie'
set {
    friends := assert_distinct((
        for t in {
            ('Cameron', 1),
            ('Dana', 2),
            ('Elsa', 3),
            ('Zoe', 4),
        }
        select U {@weight := t.1} filter .name = t.0
    ))
};
with U := User
update User
filter .name = 'Cameron'
set {
    friends := assert_distinct((
        for t in {
            ('Alice', 1),
            ('Billie', 1),
            ('Dana', 1),
            ('Elsa', 1),
            ('Zoe', 1),
        }
        select U {@weight := t.1} filter .name = t.0
    ))
};
with U := User
update User
filter .name = 'Elsa'
set {
    friends := assert_distinct((
        for t in {
            ('Zoe', 10),
        }
        select U {@weight := t.1} filter .name = t.0
    ))
};
with U := User
update User
filter .name = 'Zoe'
set {
    friends := assert_distinct((
        for t in {('Elsa', 5), ('Alice', 100)}
        select U {@weight := t.1} filter .name = t.0
    ))
};

insert UserGroup {
    name := 'red',
    users := (select User filter .name not in {'Elsa', 'Zoe'}),
};
insert UserGroup {
    name := 'green',
    users := (select User filter .name in {'Alice', 'Billie'}),
};
insert UserGroup {
    name := 'blue',
};

insert GameSession {
    num := 123,
    players := (select User filter .name in {'Alice', 'Billie'}),
};
insert GameSession {
    num := 456,
    players := (select User filter .name in {'Dana'}),
};
update GameSession
set {
    players := .players{@is_tall_enough := not contains('AEIOU', .name[0])}
};

insert Post {
    author := assert_single((select User filter .name = 'Alice')),
    body := 'Hello',
};
insert Post {
    author := assert_single((select User filter .name = 'Alice')),
    body := "I'm Alice",
};
insert Post {
    author := assert_single((select User filter .name = 'Cameron')),
    body := "I'm Cameron",
};
insert Post {
    author := assert_single((select User filter .name = 'Elsa')),
    body := '*magic stuff*',
};

insert AssortedScalars {
    name:= 'hello world',
    vals := ['brown', 'fox'],
    bstr := b'word\x00\x0b',
    time := <cal::local_time>'20:13:45.678',
    date:= <cal::local_date>'2025-01-26',
    ts:=<datetime>'2025-01-26T20:13:45+00:00',
    lts:=<cal::local_datetime>'2025-01-26T20:13:45',
};