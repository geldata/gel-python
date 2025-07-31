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
    public := true,
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

insert KitchenSink {
    str := 'hello world',
    p_multi_str := {'brown', 'fox'},

    array := ["foo"],
    p_multi_arr := {["foo"], ["bar"]},

    p_arrtup := [("foo",)],
    p_multi_arrtup := {[("foo",)], [("bar",)]},

    p_tuparr := (["foo"],),
    p_multi_tuparr := {(["foo"],), (["bar"],)},
};

insert KitchenSink {
    str := "another one",
    p_multi_str := {"quick", "fox", "jumps"},

    array := ["bar"],
    p_multi_arr := {["foo", "bar"], ["baz"]},

    p_arrtup := [("foo",), ("bar",)],
    p_multi_arrtup := {[("baz",)], [("qux",)]},

    p_tuparr := (["bar"],),
    p_multi_tuparr := {(["bar"],), (["baz"],)},
};

insert RangeTest {
    name := 'test range',
    int_range := range(23, 45),
    float_range := range(2.5, inc_lower := false),
    date_range := range(
        <cal::local_date>'2025-01-06',
        <cal::local_date>'2025-02-17',
    ),
};

insert MultiRangeTest {
    name := 'test multirange',
    int_mrange := multirange([
        range(2, 4), range(23, 45)
    ]),
    float_mrange := multirange([
        range(2.5, inc_lower := false), range(0, 0.5)
    ]),
    date_mrange := multirange([
        range(
            <cal::local_date>'2025-01-06',
            <cal::local_date>'2025-02-17',
        ),
        range(
            <cal::local_date>'2025-03-16',
        ),
    ]),
};

insert EnumTest {name := 'red', color := Color.Red};
insert EnumTest {name := 'green', color := Color.Green};
insert EnumTest {name := 'blue', color := Color.Blue};
