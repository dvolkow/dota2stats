CREATE TABLE dota2.matches (
    start_time          DateTime,
    match_id            UInt64,
    lobby_type          LowCardinality(String),
    game_mode           LowCardinality(String),
    radiant_score       UInt16,
    dire_score          UInt16,
    cluster             UInt16,
    radiant_win         LowCardinality(String),
    duration            UInt16,
    skill               LowCardinality(String),
    account_id          UInt64,
    player_slot         UInt16,
    hero_id             UInt8,
    item_neutral        UInt16,
    kills               UInt16,
    deaths              UInt16,
    assists             UInt16,
    leaver_status       LowCardinality(String),
    last_hits           UInt16,
    denies              UInt16,
    gold_per_min        UInt16,
    xp_per_min          UInt16,
    level               UInt8,
    net_worth           UInt32,
    aghanims_scepter    LowCardinality(String),
    aghanims_shard      LowCardinality(String),
    moonshard           LowCardinality(String),
    hero_damage         UInt32,
    tower_damage        UInt32,
    hero_healing        UInt32,
    gold                UInt32,
    gold_spent          UInt32,
    scaled_hero_damage  UInt32,
    scaled_tower_damage UInt32,
    scaled_hero_healing UInt32
) ENGINE = MergeTree()
PRIMARY KEY (start_time, match_id, account_id)
ORDER BY (start_time, match_id, account_id)
SETTINGS index_granularity = 8192;


CREATE TABLE dota2.items (
    actual_date         DateTime DEFAULT now(),
    id                  UInt16,
    name                String,
    cost                UInt16,
    secret_shop         UInt8,
    side_shop           UInt8,
    recipe              UInt8
) ENGINE = MergeTree()
PRIMARY KEY id
ORDER BY id
SETTINGS index_granularity = 8192;


CREATE TABLE dota2.heroes (
    actual_date         DateTime DEFAULT now(),
    id                  UInt16,
    name                String
) ENGINE = MergeTree()
PRIMARY KEY id
ORDER BY id
SETTINGS index_granularity = 8192;