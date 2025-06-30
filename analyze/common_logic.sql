-- Common Table Expressions for Plate Clustering
-- This script is not meant to be run directly. It's included by the main analysis scripts.

-- Step 1: Calculate the size of each plate
plate_sizes AS (
    SELECT
        plate_code,
        COUNT(ticker) AS num_stocks
    FROM stock_plate_mappings
    GROUP BY plate_code
),

-- Step 2: For each stock, rank its plates by size (smallest first)
ranked_plates AS (
    SELECT
        sm.ticker,
        sm.plate_name,
        ROW_NUMBER() OVER(PARTITION BY sm.ticker ORDER BY ps.num_stocks ASC, sm.plate_name ASC) as rnk
    FROM stock_plate_mappings sm
    JOIN plate_sizes ps ON sm.plate_code = ps.plate_code
),

-- Step 3: Select the smallest plate for each stock and apply clustering logic
clustered_plate_data AS (
    SELECT
        ticker,
        plate_name AS smallest_plate,
        CASE
            WHEN plate_name IN ('医疗设备及用品', '医疗及医学美容服务', '医药外包概念', '医疗保健', '中医药', '中医药概念', '药品', '药品分销', '生物技术', '生物医药', '生物医药B类股', '创新药概念', 'AI医疗概念股', '互联网医疗', '医美概念股') THEN '医疗与健康'
            WHEN plate_name IN ('地产投资', '地产发展商', '楼宇建造', '内房股', '内地物业管理股', '物业服务及管理', '建筑材料', '建材水泥股', '地产代理', '房地产基金', '房地产投资信托', '养老概念') THEN '地产与建筑'
            WHEN plate_name IN ('工业零件及器材', '重型机械', '重型机械股', '特殊化工用品', '钢铁', '其他金属及矿物', '铝', '铜', '煤炭股', '印刷及包装', '电力设备股', '半导体设备与材料') THEN '工业与制造'
            WHEN plate_name IN ('油气设备与服务', '油气生产商', '石油与天然气', '新能源物料', '非传统/可再生能源', '风电股', '光伏太阳能股', '氢能源概念股', '电池', '能源储存装置', '环保', '环保工程', '水务', '水务股', '燃气供应', '燃气股') THEN '能源与环保'
            WHEN plate_name IN ('消费电子产品', '家具', '服装', '服装零售商', '纺织品及布料', '鞋类', '珠宝钟表', '奢侈品品牌股', '餐饮', '食品股', '包装食品', '食品添加剂', '农产品', '乳制品', '酒精饮料', '非酒精饮料', '啤酒', '超市及便利店', '百货业股', '其他零售商', '线上零售商', '国内零售股') THEN '消费品与零售'
            WHEN plate_name IN ('OLED概念', 'LED', '电讯设备', '应用软件', '电脑及周边器材', '芯片股', '半导体', '5G概念', 'ChatGPT概念股', '元宇宙概念', '机器人概念股', '智能驾驶概念股', '云计算', 'SaaS概念', '手游股', '游戏软件', '短视频概念股', '抖音概念股', '腾讯概念', '阿里概念股', '小米概念', '苹果概念') THEN '科技与创新'
            WHEN plate_name IN ('公共运输', '航运及港口', '港口运输股', '物流', '航空服务', '航空货运及物流', '公路及铁路股', '高铁基建股', '一带一路', '重型基建') THEN '交通运输与物流'
            WHEN plate_name IN ('职业教育', 'K12教育', '民办高教', '内地教育股', '在线教育', '教育', '其他支援服务', '采购及供应链管理') THEN '教育与服务'
            WHEN plate_name IN ('内银股', '银行', '保险', '保险股', '证券及经纪', '中资券商股', '投资及资产管理', '信贷', '其他金融', '高股息概念') THEN '金融与投资'
            WHEN plate_name IN ('赌场及博彩', '博彩股', '影视娱乐', '影视股', '玩具及消闲用品', '旅游及观光', '酒店及度假村', '消闲及文娱设施') THEN '娱乐与休闲'
            WHEN plate_name IN ('汽车零件', '汽车零售商', '汽车经销商', '新能源车企', '特斯拉概念股', '综合车企股', '商业用车及货车') THEN '汽车与配件'
            WHEN plate_name IN ('MSCI中国大陆小型股', 'MSCI中国香港小型股', '红海危机概念', '双十一', '昨日强势股', '港股通(沪)', '红筹股', '蓝筹股') THEN '其他概念股'
            ELSE '其他'
        END AS plate_cluster
    FROM ranked_plates
    WHERE rnk = 1
) 