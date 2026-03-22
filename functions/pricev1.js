// Cloudflare Workers - price.js
// 部署到 https://dash.cloudflare.com → Workers & Pages → Create Worker

const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, X-App-Version',
  'Content-Type': 'application/json',
};

// 最低允许版本，提高此数字即可封禁旧版 APK
const MIN_APP_VERSION = 2;

export default {
  async fetch(request, env) {
    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: CORS_HEADERS });
    }
    if (request.method !== 'POST') {
      return new Response('Method not allowed', { status: 405 });
    }

    const clientVersion = parseInt(request.headers.get('X-App-Version') || '1', 10);
    if (clientVersion < MIN_APP_VERSION) {
      return json({ error: '当前版本已停用，请更新应用' }, 403);
    }

    const clientToken = request.headers.get('X-App-Token');
    if (!env.APP_TOKEN || clientToken !== env.APP_TOKEN) {
      return json({ error: 'Unauthorized' }, 401);
    }

    const url = new URL(request.url);
    const DOUBAO_API_KEY = env.DOUBAO_API_KEY;
    if (!DOUBAO_API_KEY) return json({ error: 'DOUBAO_API_KEY not set' }, 500);

    // /activate 路由：激活码验证
    if (url.pathname === '/activate') {
      try {
        const { key } = await request.json();
        if (!key) return json({ error: '请输入激活码' }, 400);
        const kvVal = await env.PRICE_CACHE.get(`license:${key}`);
        if (!kvVal) return json({ error: '激活码无效，请检查后重试' }, 400);
        const licData = JSON.parse(kvVal);
        if (licData.activated) return json({ error: '该激活码已被使用' }, 400);
        licData.activated = true;
        licData.activatedAt = new Date().toISOString().slice(0, 10);
        await env.PRICE_CACHE.put(`license:${key}`, JSON.stringify(licData));
        return json({ success: true });
      } catch (e) {
        return json({ error: e.message }, 500);
      }
    }

    // /chat 路由：保留兼容旧版本
    if (url.pathname === '/chat') {
      try {
        const body = await request.json();
        const res = await fetch('https://ark.cn-beijing.volces.com/api/v3/responses', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${DOUBAO_API_KEY}` },
          body: JSON.stringify(body),
        });
        const data = await res.json();
        return json(data);
      } catch (error) {
        return json({ error: error.message }, 500);
      }
    }

    // 默认路由：价格查询
    try {
      const { category, name } = await request.json();
      if (!name) return json({ error: 'Missing name' }, 400);

      if (!category) return json({ error: 'Missing category' }, 400);

      // 分红数据：返回 dividend:ALL blob
      if (category === '__dividends__') {
        const blob = await env.PRICE_CACHE.get('dividend:ALL');
        return json({ data: blob ? JSON.parse(blob) : {} });
      }

      // 汇率数据：返回 fx:ALL blob
      if (category === '__fx__') {
        const blob = await env.PRICE_CACHE.get('fx:ALL');
        return json({ data: blob ? JSON.parse(blob) : {} });
      }

      // 本地缓存下载：返回完整 KV blob，供 APP 本地缓存
      // 直接注入原始字符串，避免大 blob 双重解析导致 CPU 超限
      if (category === '__blob__') {
        const blobMap = { stock_cn: 'stock_cn:ALL', stock_hk: 'stock_hk:ALL', stock_us: 'stock_us:ALL', fund: 'fund:ALL', gold: 'gold:ALL', crypto: 'crypto:ALL' };
        const kvKey = blobMap[name];
        if (!kvKey) return json({ error: 'unknown blob type' }, 400);
        const blobStr = await env.PRICE_CACHE.get(kvKey);
        return new Response(`{"data":${blobStr ?? '{}'}}`, { headers: CORS_HEADERS });
      }

      // 股票/基金/加密货币/黄金直接路由，不走独立 key 缓存
      const codeCategories = ['stock_cn', 'stock_hk', 'stock_us', 'fund', 'crypto', 'gold'];
      if (codeCategories.includes(category)) {
        switch (category) {
          case 'stock_cn': return getStockCN(name, env);
          case 'stock_hk': return getStockHK(name, env);
          case 'stock_us': return getStockUS(name, env);
          case 'fund':     return getFund(name, env);
          case 'crypto':   return getCrypto(name);
          case 'gold':     return getGold(env);
        }
      }

      // Step 1: 非股票类查 KV 缓存
      const cacheKey = `${category}:${name.toLowerCase()}`;
      if (env.PRICE_CACHE) {
        const cached = await env.PRICE_CACHE.get(cacheKey);
        if (cached) {
          console.log(`[KV] 查「${name}」价格 → 命中`);
          const cachedData = JSON.parse(cached);
          if (cachedData.note) cachedData.note = cachedData.note.replace(/·\s*实时\s*$/, '· 缓存');
          return json(cachedData);
        } else {
          console.log(`[KV] 查「${name}」价格 → 未命中，准备请求数据源`);
        }
      }

      // Step 3: 其他资产类别（realestate/car/other）走 Doubao 联网搜索
      // 有 License Key 时验证；无 key 时依赖 APP 侧试用次数控制，直接放行
      const licKey = request.headers.get('X-License-Key');
      if (licKey) {
        const licKV = await env.PRICE_CACHE.get(`license:${licKey}`);
        if (!licKV) return json({ error: '激活码无效', code: 'LICENSE_INVALID' }, 402);
        const licData = JSON.parse(licKV);
        if (!licData.activated) return json({ error: '激活码未激活', code: 'LICENSE_INVALID' }, 402);
      }

      const response = await getByDoubao(name, category, DOUBAO_API_KEY);

      // Step 4: 写入 KV 缓存（按类别设不同 TTL）
      if (env.PRICE_CACHE) {
        try {
          const data = await response.clone().json();
          if (data.price > 0) {
            const ttl = getTTL(category);
            await env.PRICE_CACHE.put(cacheKey, JSON.stringify(data), { expirationTtl: ttl });
            console.log(`[KV] 写入「${name}」价格 ${data.price}，TTL=${ttl}s`);
          }
        } catch (e) {}
      }

      return response;
    } catch (error) {
      return json({ error: error.message }, 500);
    }
  }
};

function json(data, status = 200) {
  return new Response(JSON.stringify(data), { status, headers: CORS_HEADERS });
}

// 不同资产类别的 KV 缓存 TTL
function getTTL(category) {
  if (['realestate', 'car', 'other'].includes(category)) return 302400; // 慢变资产：84小时
  return 86400; // 股票/基金/加密货币：24小时
}


// A股：仅接受6位数字代码，直接查 ALL blob，miss 则查东方财富实时
async function getStockCN(name, env) {
  const code = name.trim();
  if (!/^\d{6}$/.test(code)) return json({ error: 'A股代码须为6位数字' }, 400);

  try {
    if (env?.PRICE_CACHE) {
      const allBlob = await env.PRICE_CACHE.get('stock_cn:ALL');
      if (allBlob) {
        const entry = JSON.parse(allBlob)[code];
        if (entry) {
          console.log(`[KV] A股 ALL blob 命中 ${code}`);
          return json(entry);
        }
      }
    }

    const market = code.startsWith('6') ? 1 : 0;
    const priceRes = await fetch(
      `https://push2.eastmoney.com/api/qt/stock/get?secid=${market}.${code}&fields=f43,f58,f170`
    );
    const priceData = await priceRes.json();
    const currentPrice = (priceData?.data?.f43 ?? 0) / 100;
    const stockName = priceData?.data?.f58;
    const changePct = ((priceData?.data?.f170 ?? 0) / 100).toFixed(2);
    if (!currentPrice || currentPrice <= 0) return json({ error: `代码 ${code} 未找到，请确认后重试` }, 404);

    console.log(`[东财] A股 ${code} 实时价 ${currentPrice}`);
    return json({
      price: currentPrice, unit: '元/股',
      note: `${stockName} 实时价，较昨收${parseFloat(changePct) > 0 ? '+' : ''}${changePct}% · 实时`,
      confidence: 'high', name: stockName,
    });
  } catch (e) {
    console.log(`[东财] 查A股 ${code} 出错: ${e.message}`);
    return json({ error: '查询失败，请稍后重试' }, 500);
  }
}

// 美股：仅接受1-5位字母ticker，直接查 ALL blob，miss 则查东方财富实时
async function getStockUS(name, env) {
  const ticker = name.trim().toUpperCase();
  if (!/^[A-Z]{1,5}$/.test(ticker)) return json({ error: '美股代码须为1-5位字母，如：AAPL' }, 400);

  try {
    if (env?.PRICE_CACHE) {
      const allBlob = await env.PRICE_CACHE.get('stock_us:ALL');
      if (allBlob) {
        const entry = JSON.parse(allBlob)[ticker];
        if (entry) {
          console.log(`[KV] 美股 ALL blob 命中 ${ticker}`);
          return json(entry);
        }
      }
    }

    let priceData = null;
    for (const market of ['105', '106']) {
      const priceRes = await fetch(
        `https://push2.eastmoney.com/api/qt/stock/get?secid=${market}.${ticker}&fields=f43,f58,f170`
      );
      const data = await priceRes.json();
      if (data?.data?.f43 > 0) { priceData = data.data; break; }
    }
    if (!priceData) return json({ error: `代码 ${ticker} 未找到，请确认后重试` }, 404);

    const usdPrice = priceData.f43 / 1000;
    const stockName = priceData.f58;
    const changePct = (priceData.f170 / 100).toFixed(2);
    console.log(`[东财] 美股 ${ticker} 实时价 $${usdPrice}`);
    return json({
      price: usdPrice, unit: '美元/股',
      note: `${stockName} 较昨收${parseFloat(changePct) > 0 ? '+' : ''}${changePct}% · 实时`,
      confidence: 'high', name: stockName,
    });
  } catch (e) {
    console.log(`[东财] 查美股 ${ticker} 出错: ${e.message}`);
    return json({ error: '查询失败，请稍后重试' }, 500);
  }
}

// 港股：仅接受1-5位数字代码，补零到5位，直接查 ALL blob，miss 则查东方财富实时
async function getStockHK(name, env) {
  if (!/^\d{5}$/.test(name.trim())) return json({ error: '港股代码须为5位数字，如：00700' }, 400);
  const code = name.trim();

  try {
    if (env?.PRICE_CACHE) {
      const allBlob = await env.PRICE_CACHE.get('stock_hk:ALL');
      if (allBlob) {
        const entry = JSON.parse(allBlob)[code];
        if (entry) {
          console.log(`[KV] 港股 ALL blob 命中 ${code}`);
          return json(entry);
        }
      }
    }

    const priceRes = await fetch(
      `https://push2.eastmoney.com/api/qt/stock/get?secid=116.${code}&fields=f43,f58,f170`
    );
    const data = await priceRes.json();
    const hkdPrice = (data?.data?.f43 ?? 0) / 1000;
    const stockName = data?.data?.f58;
    const changePct = ((data?.data?.f170 ?? 0) / 100).toFixed(2);
    if (!hkdPrice || hkdPrice <= 0) return json({ error: `代码 ${code} 未找到，请确认后重试` }, 404);

    console.log(`[东财] 港股 ${code} 实时价 HK$${hkdPrice}`);
    return json({
      price: hkdPrice, unit: '港元/股',
      note: `${stockName} 较昨收${parseFloat(changePct) > 0 ? '+' : ''}${changePct}% · 实时`,
      confidence: 'high', name: stockName,
    });
  } catch (e) {
    console.log(`[东财] 查港股 ${code} 出错: ${e.message}`);
    return json({ error: '查询失败，请稍后重试' }, 500);
  }
}

// 基金：仅接受6位数字代码，直接查 ALL blob，miss 则查 fundgz 实时估值
async function getFund(name, env) {
  const code = name.trim();
  if (!/^\d{6}$/.test(code)) return json({ error: '基金代码须为6位数字，如：110010' }, 400);

  try {
    if (env?.PRICE_CACHE) {
      const allBlob = await env.PRICE_CACHE.get('fund:ALL');
      if (allBlob) {
        const entry = JSON.parse(allBlob)[code];
        if (entry) {
          console.log(`[KV] 基金 ALL blob 命中 ${code}`);
          return json(entry);
        }
      }
    }

    const priceRes = await fetch(`https://fundgz.1234567.com.cn/js/${code}.js?rt=${Date.now()}`);
    const priceText = await priceRes.text();
    const priceMatch = priceText.match(/jsonpgz\((\{.*\})\)/);
    if (!priceMatch) return json({ error: `代码 ${code} 未找到，请确认后重试` }, 404);

    const fundData = JSON.parse(priceMatch[1]);
    const price = parseFloat(fundData.gsz || fundData.dwjz);
    if (!price || price <= 0) return json({ error: `代码 ${code} 未找到，请确认后重试` }, 404);

    console.log(`[基金] ${code} 实时估值 ${price}`);
    return json({
      price, unit: '元/份',
      note: `${fundData.name} ${fundData.gsz ? '实时估值' : '最新净值'} · 实时`,
      confidence: fundData.gsz ? 'high' : 'medium', name: fundData.name,
    });
  } catch (e) {
    console.log(`[基金] 查 ${code} 出错: ${e.message}`);
    return json({ error: '查询失败，请稍后重试' }, 500);
  }
}

// 加密货币：CoinGecko 实时查询，不走 KV 缓存
async function getCrypto(name) {
  try {
    const searchRes = await fetch(
      `https://api.coingecko.com/api/v3/search?query=${encodeURIComponent(name)}`
    );
    const searchData = await searchRes.json();
    const coinId = searchData?.coins?.[0]?.id;
    const coinName = searchData?.coins?.[0]?.name;
    if (!coinId) return json({ error: `未找到加密货币「${name}」，请检查名称后重试` }, 404);

    const priceRes = await fetch(
      `https://api.coingecko.com/api/v3/simple/price?ids=${coinId}&vs_currencies=cny&include_24hr_change=true`
    );
    const priceData = await priceRes.json();
    const price = priceData?.[coinId]?.cny;
    const change = priceData?.[coinId]?.cny_24h_change?.toFixed(2);
    if (!price) return json({ error: `未找到加密货币「${name}」价格，请稍后重试` }, 404);

    return json({
      price, unit: '元/枚',
      note: `${coinName} 实时价，24h ${change >= 0 ? '+' : ''}${change}%`,
      confidence: 'high', name: coinName,
    });
  } catch (e) {
    return json({ error: '查询失败，请稍后重试' }, 500);
  }
}

// 黄金：从 gold:ALL blob 读取 Au99.99 现货价
async function getGold(env) {
  try {
    if (env?.PRICE_CACHE) {
      const blob = await env.PRICE_CACHE.get('gold:ALL');
      if (blob) {
        const data = JSON.parse(blob);
        const gold = data['Au99.99'];
        if (gold?.price > 0) {
          console.log(`[KV] gold:ALL 命中，价格 ${gold.price}`);
          return json(gold);
        }
      }
    }
    return json({ error: '黄金价格暂未同步，请稍后重试' }, 503);
  } catch (e) {
    return json({ error: '查询失败，请稍后重试' }, 500);
  }
}

// Doubao 联网搜索（房产/车辆/其他）—— 单轮，服务端执行搜索
async function getByDoubao(name, category, apiKey) {
  if (!apiKey) return json({ error: 'DOUBAO_API_KEY not set' }, 500);
  console.log(`[Doubao] 查「${name}」价格，分类=${category}`);

  const systemPrompt = `你是资产估价助手。必须联网搜索获取最新价格。
只返回JSON，不要任何其他文字，不要markdown代码块：
{"price":数字,"unit":"单位","note":"一句话说明","confidence":"high/medium/low","name":"资产标准名称"}
name字段填写该资产的官方/标准名称，例如房产填小区名，车辆填"2022款丰田凯美瑞"。
价格必须换算为人民币元（整数），unit字段固定为"元"，严禁使用万元、美元、港元、盎司等单位。`;

  const userContent = category === 'car'
    ? `资产类别：${category}，资产名称：${name}，请查询该车型当前二手车市场均价（非新车指导价）`
    : `资产类别：${category}，资产名称：${name}`;

  const res = await fetch('https://ark.cn-beijing.volces.com/api/v3/responses', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${apiKey}` },
    body: JSON.stringify({
      model: 'doubao-seed-2-0-lite-260215',
      instructions: systemPrompt,
      input: [{ type: 'message', role: 'user', content: userContent }],
      tools: [{ type: 'web_search', limit: 3, sources: ['search_engine'] }],
      max_output_tokens: 512,
    }),
  });

  if (!res.ok) {
    const errBody = await res.text();
    throw new Error(`Doubao error ${res.status}: ${errBody.slice(0, 300)}`);
  }

  const data = await res.json();
  const messageItem = data.output?.find(item => item.type === 'message');
  const content = messageItem?.content?.[0]?.text ?? '';
  const match = content.match(/\{[\s\S]*\}/);
  if (!match) throw new Error(`No JSON in Doubao response: ${content.slice(0, 200)}`);

  const priceData = JSON.parse(match[0]);
  if (typeof priceData.price === 'string') {
    priceData.price = parseFloat(priceData.price.replace(/[^\d.]/g, ''));
  }
  // 万元 → 元
  if (priceData.unit && priceData.unit.includes('万')) {
    priceData.price = Math.round(priceData.price * 10000);
    priceData.unit = priceData.unit.replace('万元', '元').replace('万', '元');
  }
  if (priceData.note && !priceData.note.includes('·')) priceData.note += ' · Doubao';
  return json(priceData);
}
