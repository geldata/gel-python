insert User {name := 'Alice'};
insert User {name := 'Billie'};
insert User {name := 'Cameron'};
insert User {name := 'Dana'};
insert User {name := 'Elsa'};
insert User {name := 'Zoe'};

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

insert Child {num := 0};
insert Child {num := 1};

insert HasLinkPropsA {
    child := (select Child{@a := 'single'} filter .num = 0)
};

insert HasLinkPropsB;
update HasLinkPropsB
set {
    children += (select Child{@b := 'hello'} filter .num = 0)
};
update HasLinkPropsB
set {
    children += (select Child{@b := 'world'} filter .num = 1)
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