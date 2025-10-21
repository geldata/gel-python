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

# Poly data
insert Person {
    name := 'Alice',
    game_id := 1,
};
insert Person {
    name := 'Billie',
    game_id := 2,
    item := (
        insert Bag {name := 'nice bag', game_id := 11}
    ),
};
insert Person {
    name := 'Cameron',
    game_id := 3,
    item := (
        insert Box {
            name := 'big box',
            game_id := 11,
            contents := {
                # Same type, directly extending abstract Content
                (insert Candy {name := 'cotton candy', game_id := 101}),
                (insert Candy {name := 'candy corn', game_id := 102}),
            }
        }
    ),
};
insert Person {
    name := 'Dana',
    game_id := 4,
    item := (
        insert Tin {
            name := 'round tin',
            game_id := 12,
            contents := {
                # Tin has to contain Chocolate
                (insert Chocolate {
                    name := 'milk', kind := 'bar', game_id := 3001
                }),
                (insert Chocolate {
                    name := 'dark', kind := 'truffle', game_id := 3002
                }),
            }
        }
    ),
};
insert Person {
    name := 'Elsa',
    game_id := 5,
    item := (
        insert Box {
            name := 'package',
            game_id := 13,
            contents := {
                (insert Candy {name := 'lemon drop', game_id := 103}),
                (insert Chocolate {
                    name := 'almond', kind := 'piece', game_id := 3003
                }),
                (insert Gummy {
                    name := 'blue bear', flavor := 'grape', game_id := 1001
                }),
                (insert GummyWorm {
                    name := 'sour worm',
                    game_id := 2002,
                    flavor := 'raspberry',
                    size := 2,
                }),
            }
        }
    ),
};
insert Person {
    name := 'Zoe',
    game_id := 6,
    item := (
        insert GiftBox {
            name := 'fancy',
            game_id := 14,
            contents := {
                (insert GummyWorm {
                    name := 'sour worm',
                    game_id := 2001,
                    flavor := 'fruity',
                    size := 10,
                }),
            }
        }
    ),
};

insert Inh_A {
    a := 1,
};
insert Inh_B {
    b := 2,
};
insert Inh_C {
    c := 3,
};
insert Inh_AB {
    a := 4,
    b := 5,
    ab := 6,
};
insert Inh_AC {
    a := 7,
    c := 8,
    ac := 9,
};
insert Inh_BC {
    b := 10,
    c := 11,
    bc := 12,
};
insert Inh_ABC {
    a := 13,
    b := 14,
    c := 15,
    abc := 16,
};
insert Inh_AB_AC {
    a := 17,
    b := 18,
    c := 19,
    ab := 20,
    ac := 21,
    ab_ac := 22,
};

insert Inh_XA {
    a := 1000,
};
insert Inh_AXA {
    a := 1001,
    axa := 10002,
};

insert Link_Inh_A {
    n := 1,
    l := assert_exists((select Inh_A filter .a = 1 limit 1)),
};
insert Link_Inh_A {
    n := 4,
    l := assert_exists((select Inh_AB filter .a = 4 limit 1)),
};
insert Link_Inh_A {
    n := 7,
    l := assert_exists((select Inh_AC filter .a = 7 limit 1)),
};
insert Link_Inh_A {
    n := 13,
    l := assert_exists((select Inh_ABC filter .a = 13 limit 1)),
};
insert Link_Inh_A {
    n := 17,
    l := assert_exists((select Inh_AB_AC filter .a = 17 limit 1)),
};
