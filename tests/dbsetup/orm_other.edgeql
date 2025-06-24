insert User {name := 'Alice'};
insert User {name := 'Billie'};
insert User {name := 'Cameron'};
insert User {name := 'Dana'};
insert User {name := 'Elsa'};
insert User {name := 'Zoe'};

insert AssortedScalars {
    name := 'hello world',

    date := <cal::local_date>'2025-01-26',
    ts := <datetime>'2025-01-26T20:13:45+00:00',
    lts := <cal::local_datetime>'2025-01-26T20:13:45',
    time := <cal::local_time>'20:13:45.678',
    json := to_json(
        '["hello", {"name": "John Doe", "age": 42, "special": null}, false]'),
    bstr := b'word\x00\x0b',

    nested_mixed := [
        ([1, 1, 2, 3], to_json('{"label": "Fibonacci sequence", "next": 5}')),
        ([123, 0, 0, 3], to_json('"simple JSON"')),
        (<array<int64>>[], to_json('null')),
    ],

    positive := 123,
};

insert EnumTest {name := 'red', color := Color.Red};
insert EnumTest {name := 'green', color := Color.Green};
insert EnumTest {name := 'blue', color := Color.Blue};
