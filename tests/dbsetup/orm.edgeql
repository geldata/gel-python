insert User {name := 'Alice'};
insert User {name := 'Billie'};
insert User {name := 'Cameron'};
insert User {name := 'Dana'};
insert User {name := 'Elsa'};
insert User {name := 'Zoe'};

insert UserGroup {
    name := 'red',
    users := (select User filter .name not in {'Elsa', 'Zoe'}),
    mascot := 'dragon',
};
insert UserGroup {
    name := 'green',
    users := (select User filter .name in {'Alice', 'Billie'}),
    mascot := 'wombat',
};
insert UserGroup {
    name := 'blue',
    mascot := 'dolphin',
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
    json := to_json(
        '["hello", {"name": "John Doe", "age": 42, "special": null}, false]'),
    bstr := b'word\x00\x0b',
    time := <cal::local_time>'20:13:45.678',
    date:= <cal::local_date>'2025-01-26',
    ts:=<datetime>'2025-01-26T20:13:45+00:00',
    lts:=<cal::local_datetime>'2025-01-26T20:13:45',
};

insert NestedScalars {
    name:= 'hello world',
    nested_array := [(1, 'a'), (2, 'big'), (3, 'cat')],
    nested_tuple := ((99, 'beers on the wall'), true),
    nested_mixed := [
        ([1, 1, 2, 3], to_json('{"label": "Fibonacci sequence", "next": 5}')),
        ([123, 0, 0, 3], to_json('"simple JSON"')),
        (<array<int64>>[], to_json('null')),
    ],
};

insert Image {
    file := 'cat.jpg',
    author := assert_single((
        select User {
            @caption := 'made of snow',
            @year := 2025,
        }
        filter .name = 'Elsa'
    ))
};

insert Loot {
    name := 'Cool Hat',
    owner := assert_single((
        select User filter .name = 'Billie'
    )),
};

insert StackableLoot {
    name := 'Gold Coin',
    owner := assert_single((
        select User {
            @count := 34,
            @bonus := True,
        } filter .name = 'Billie'
    )),
};
