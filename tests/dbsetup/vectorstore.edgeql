for i in range_unpack(range(1, 8))
union (
    insert ext::vectorstore::DefaultRecord {
        collection := "test",
        external_id := "00000000-0000-0000-0000-00000000000" ++ <str>i,
        text := "some text",
        embedding := <ext::pgvector::vector>(array_fill(1, i * 192) ++ array_fill(0, (8 - i) * 192)),
        metadata := to_json('{ "str_field": ' ++  ('"least_similar"' if i <= 4 else '"most_similar"') ++ '}'),
    }
);

