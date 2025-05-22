with 
strategies as (
    with const_data AS (
        SELECT * FROM (VALUES
            ('0xbeac0eeeeeeeeeeeeeeeeeeeeeeeeeeeeeebeac0', 'WETH', '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'),
            ('0x54945180db7943c0ed0fee7edab2bd24620256bc', 'cbETH', '0xBe9895146f7AF43049ca1c1AE358B0541Ea49704'),
            ('0x93c4b944d05dfe6df7645a86cd2206016c51564d', 'stETH', '0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84'),
            ('0x1bee69b7dfffa4e2d53c2a2df135c388ad25dcd2', 'rETH', '0xae78736Cd615f374D3085123A210448E74Fc6393'),
            ('0x7ca911e83dabf90c90dd3de5411a10f1a6112184', 'wBETH', '0xa2E3356610840701BDf5611a53974510Ae27E2e1'),
            ('0x0fe4f44bee93503346a3ac9ee5a26b130a5796d6', 'swETH', '0xf951E335afb289353dc249e82926178EaC7DEd78'),
            ('0x57ba429517c3473b6d34ca9acd56c0e735b94c02', 'osETH', '0xf1C9acDc66974dFB6dEcB12aA385b9cD01190E38'),
            ('0xa4c637e0f704745d182e4d38cab7e7485321d059', 'OETH', '0x856c4Efb76C1D1AE02e20CEB03A2A6a08b0b8dC3'),
            ('0x13760f50a9d7377e4f20cb8cf9e4c26586c658ff', 'ankETH', '0xE95A203B1a91a908F9B9CE46459d101078c2c3cb'),
            ('0x9d7ed45ee2e8fc5482fa2428f15c971e6369011d', 'ETHx', '0xA35b1B31Ce002FBF2058D22F30f95D405200A15b'),
            ('0x298afb19a105d59e74658c4c334ff360bade6dd2', 'mETH', '0xd5F7838F5C461fefF7FE49ea5ebaF7728bB0ADfa'),
            ('0xae60d8180437b5c34bb956822ac2710972584473', 'LsETH', '0x8c1BEd5b9a0928467c9B1341Da1D7BD5e10b6549'),
            ('0x8ca7a5d6f3acd3a7a8bc468a8cd0fb14b6bd28b6', 'sfrxETH', '0xac3E018457B222d93114458476f3E3416Abbe38F'),
            ('0xacb55c530acdb2849e6d4f36992cd8c9d50ed8f7', 'bEIGEN', '0x83E9115d334D248Ce39a6f36144aEaB5b3456e75')
        ) AS t(strategy, symbol, underlying_token)
    ),
    log_data AS (
        SELECT
            '0x89b22cb153cf744ee313c64140c2971ea72794f874e40f28704be6a2b5eb9974' AS evt_tx_hash,
            0 AS evt_index,
            CAST('2023-06-09 10:16:59' AS TIMESTAMP) AS evt_block_time,
            17445570 AS evt_block_number,
            '0xbeac0eeeeeeeeeeeeeeeeeeeeeeeeeeeeeebeac0' AS strategy
        UNION ALL
        SELECT
            tx_hash,
            event_index,
            block_timestamp,
            block_number,
            decoded_log:strategy::STRING AS strategy
        FROM ethereum.core.ez_decoded_event_logs
        WHERE
            LOWER(contract_address) = LOWER('0x858646372CC42E1A627fcE94aa7A7033e7CF075A')
            AND event_name = 'StrategyAddedToDepositWhitelist'
    )
    SELECT
        l.strategy,
        c.symbol,
        c.underlying_token
    FROM log_data l
    LEFT JOIN const_data c
        ON LOWER(l.strategy) = LOWER(c.strategy)
),
prices as (
    select * from (
        select 
            date_trunc('day', hour) as date,
            lower(token_address) as token,
            decimals,
            symbol,
            avg(price) as price
        from (
           select 
    hour,
    token_address,
    decimals,
    symbol,
    price
from ethereum.price.ez_prices_hourly
where hour > date'2023-04-01'
and blockchain = 'ethereum'
and token_address in (
    '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', '0xbe9895146f7af43049ca1c1ae358b0541ea49704',
    '0xae7ab96520de3a18e5e111b5eaab095312d7fe84', '0xae78736cd615f374d3085123a210448e74fc6393',
    '0xa2e3356610840701bdf5611a53974510ae27e2e1', '0xf951e335afb289353dc249e82926178eac7ded78',
    '0xf1c9acdc66974dfb6decb12aa385b9cd01190e38', '0x856c4efb76c1d1ae02e20ceb03a2a6a08b0b8dc3',
    '0xe95a203b1a91a908f9b9ce46459d101078c2c3cb', '0xa35b1b31ce002fbf2058d22f30f95d405200a15b',
    '0xd5f7838f5c461feff7fe49ea5ebaf7728bb0adfa', '0x8c1bed5b9a0928467c9b1341da1d7bd5e10b6549',
    '0xac3e018457b222d93114458476f3e3416abbe38f', '0x83e9115d334d248ce39a6f36144aeab5b3456e75',
    '0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0', '0x3432b6a60d23ca0dfca7761b7ab56459d9c964d0',
    '0x57e114b691db790c35207b2e685d4a43181e6061', '0x9d39a5de30e57443bff2a8307a4256c8797a3497',
    '0x18084fba666a33d37592fa2633fd49a74dd93a88', '0x8236a87084f8b84306f72007f36f2618a5634494'
)

union all

-- wBETH price from BNB
select 
    hour,
    '0xa2e3356610840701bdf5611a53974510ae27e2e1' as token_address,
    18 as decimals,
    'WBETH' as symbol,
    price
from ethereum.price.ez_prices_hourly
where hour > date'2023-04-01'
and blockchain = 'bnb'
and lower(token_address) = lower('0x250632378e573c6be1ac2f97fcdf00515d0aa91b')

union all

-- sfrxETH derived price
select 
    date_trunc('hour', block_timestamp) as hour,
    '0xac3e018457b222d93114458476f3e3416abbe38f' as token_address,
    18 as decimals,
    'SFRXETH' as symbol,
    (TO_DOUBLE(assets)/1e18 * p.price / TO_DOUBLE(shares)/1e18) as price
from (
    select 
        block_timestamp,
        decoded_log:"assets"::STRING as assets,
        decoded_log:"shares"::STRING as shares
    from ethereum.core.ez_decoded_event_logs 
    where lower(contract_address) = lower('0xac3e018457b222d93114458476f3e3416abbe38f')
    and event_name in ('Deposit','Withdraw')
    and block_timestamp > date'2023-04-09'
    and TRY_TO_DOUBLE(decoded_log:"shares"::STRING) > 0
) q
join ethereum.price.ez_prices_hourly p 
    on date_trunc('hour', p.hour) = date_trunc('hour', q.block_timestamp)
    and p.blockchain = 'ethereum'
    and lower(p.token_address) = lower('0x5e8422345238f34275888049021821e8e08caa1f')

union all

-- LBTC using WBTC as proxy
select 
    hour,
    '0x8236a87084f8b84306f72007f36f2618a5634494' as token_address,
    8 as decimals,
    'LBTC' as symbol,
    price
from ethereum.price.ez_prices_hourly
where hour > date'2023-04-01'
and blockchain = 'ethereum'
and lower(token_address) = lower('0x2260fac5e5542a773aa44fbcfedf7c193bc2c599')

        ) base_prices
        group by 1,2,3,4
    ) all_prices
     
),
actions as (
   select
        t.tx_hash, t.event_index, t.block_timestamp, t.block_number,
        'deposit' as action,
        t.raw_amount as shares,
        t.from_address as staker,
        s.strategy,
        s.underlying_token
    from ethereum.core.ez_token_transfers t
    join strategies s 
        on lower(t.contract_address) = lower(s.underlying_token)
        and lower(t.to_address) = lower(s.strategy)
    where t.block_timestamp > date'2023-06-01'
    
    union all

    select
        t.tx_hash, t.event_index, t.block_timestamp, t.block_number,
        'withdraw' as action,
        t.raw_amount as shares,
        t.to_address as staker,
        s.strategy,
        s.underlying_token
    from ethereum.core.ez_token_transfers t
    join strategies s 
        on lower(t.contract_address) = lower(s.underlying_token)
        and lower(t.from_address) = lower(s.strategy)
    where t.block_timestamp > date'2023-06-01'

    union all

    select 
        s.tx_hash, s.event_index, s.block_timestamp, s.block_number,
        'deposit' as action,
        CAST(s.deposit_amount * 1e18 AS NUMBER(38, 0)) as shares,
        s.withdrawal_address as staker,
        '0xbeac0eeeeeeeeeeeeeeeeeeeeeeeeeeeeeebeac0' as strategy,
        '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' as underlying_token
    from ethereum.beacon_chain.ez_deposits s
    join (
        SELECT
            tx_hash,
            event_index,
            block_timestamp,
            block_number,
            decoded_log:eigenPod::STRING AS eigenPod
        FROM ethereum.core.ez_decoded_event_logs
        WHERE
            LOWER(contract_address) = LOWER('0x91E677b07F7AF907ec9a428aafA9fc14a0d3A338')
            AND event_name = 'PodDeployed'
    ) pods
    on lower(s.withdrawal_address) = lower(pods.eigenPod)

    union all

    select 
        s.tx_hash, s.event_index, s.block_timestamp, s.block_number,
        'withdraw' as action,
        0 as shares,
        s.withdrawal_address as staker,
        '0xbeac0eeeeeeeeeeeeeeeeeeeeeeeeeeeeeebeac0' as strategy,
        '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' as underlying_token
    from ethereum.beacon_chain.ez_deposits s
    join (
        SELECT
            tx_hash,
            event_index,
            block_timestamp,
            block_number,
            decoded_log:eigenPod::STRING AS eigenPod
        FROM ethereum.core.ez_decoded_event_logs
        WHERE
            LOWER(contract_address) = LOWER('0x91E677b07F7AF907ec9a428aafA9fc14a0d3A338')
            AND event_name = 'PodDeployed'
    ) pods
    on lower(s.withdrawal_address) = lower(pods.eigenPod)
),
daily_agg as (
    select 
        date_trunc('day', a.block_timestamp) as date,
        a.underlying_token,
        sum(case when action = 'deposit' then shares/1e18 else -shares/1e18 end) as shares
    from actions a 
    group by 1, 2
),
days as (
    select dateadd(day, seq4(), date'2023-06-01') as date
    from table(generator(rowcount => 1000))
    where dateadd(day, seq4(), date'2023-06-01') <= current_date()
),

daily_fillers as (
    select d.date, s.underlying_token
    from days d
    cross join (select distinct underlying_token from strategies) s
),
balance_data as (
    select 
        f.date,
        f.underlying_token,
        coalesce(d.shares, 0) as shares,
        sum(coalesce(d.shares, 0)) over (
            partition by f.underlying_token 
            order by f.date
        ) as token_shares_restaked
    from daily_fillers f 
    left join daily_agg d
        on f.date = d.date and f.underlying_token = d.underlying_token
)
select 
    b.date,
    b.underlying_token,
    b.shares,
    p.symbol,
    p.price,
    p.price / weth.price as eth_ratio,
    b.token_shares_restaked as token_shares_restaked_per_symbol,
    b.token_shares_restaked * p.price / weth.price as token_eth_restaked_per_symbol,
    b.token_shares_restaked * p.price as token_usd_restaked_per_symbol,
    sum(b.token_shares_restaked * p.price / weth.price) over (partition by b.date) as token_eth_restaked,
    sum(b.token_shares_restaked * p.price / weth.price) over (partition by b.date) / 1e6 as token_eth_restaked_counter,
    sum(b.token_shares_restaked * p.price) over (partition by b.date) as token_usd_restaked,
    sum(b.token_shares_restaked * p.price) over (partition by b.date) / 1e9 as token_usd_restaked_counter
from balance_data b
join prices p
  on p.date = b.date
  and lower(b.underlying_token) = p.token

join prices weth
  on weth.date = b.date
  and weth.token = lower('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2')


order by 1 desc;

-- ref Dune query- https://dune.com/queries/3732345/6277484 