box.cfg {
    listen = 3301
}

box.once('init_schema', function()
    box.schema.space.create('KV', {if_not_exists = true})

    box.space.KV:format({
        {name = 'key', type = 'string'},
        {name = 'value', type = 'varbinary', is_nullable = true}
    })

    box.space.KV:create_index('primary', {
        type = 'TREE',
        parts = {'key'},
        unique = true,
        if_not_exists = true
    })

    box.schema.user.create('user', {
        password = 'password',
        if_not_exists = true,
    })
    box.schema.user.grant('user', 'read,write', 'space', 'KV', {if_not_exists = true})
    box.schema.user.grant('user', 'execute', 'universe', nil, {if_not_exists = true})

    print('KV space has initialized')
end)