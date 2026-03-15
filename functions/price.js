export async function onRequest(context) {
  // 处理 CORS 预检
  if (context.request.method === 'OPTIONS') {
    return new Response(null, {
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
      }
    });
  }

  if (context.request.method !== 'POST') {
    return new Response('Method not allowed', { status: 405 });
  }

  try {
    const { category, name } = await context.request.json();
    if (!name) {
      return new Response(JSON.stringify({ error: 'Missing name' }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' }
      });
    }

    const API_KEY = context.env.KIMI_API_KEY; // EdgeOne 环境变量改为 KIMI_API_KEY
    if (!API_KEY) {
      return new Response(JSON.stringify({ error: 'KIMI_API_KEY not set' }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' }
      });
    }

    // system prompt：约束只返回 JSON，支持 auto 模式（category = 'auto' 时自动识别）
    const systemPrompt = `你是资产估价助手。必须联网搜索获取最新价格。
只返回JSON，不要任何其他文字，不要markdown代码块：
{"price":数字,"unit":"单位","note":"一句话说明","confidence":"high/medium/low","category":"stock_cn/stock_us/fund/crypto/gold/realestate/bond/cash/car/other"}

category 说明：
- stock_cn：A股
- stock_us：美股  
- fund：基金
- crypto：加密货币
- gold：黄金
- realestate：房产
- bond：债券
- cash：现金存款
- car：车辆
- other：其他资产`;

    // 用户消息：auto 模式直接发名称，普通模式带上类别
    const userMessage = (!category || category === 'auto')
      ? name
      : `资产类别：${category}，资产名称：${name}`;

    // 带重试的请求（Kimi 联网搜索较慢，超时设 20 秒）
    const fetchWithRetry = async (retries = 2) => {
      for (let i = 0; i <= retries; i++) {
        try {
          const controller = new AbortController();
          const timeoutId = setTimeout(() => controller.abort(), 20000);

          const response = await fetch('https://api.moonshot.cn/v1/chat/completions', {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'Authorization': `Bearer ${API_KEY}`,
            },
            body: JSON.stringify({
              model: 'kimi-k2.5',
              messages: [
                { role: 'system', content: systemPrompt },
                { role: 'user', content: userMessage },
              ],
              tools: [
                { type: 'builtin_function', function: { name: '$web_search' } }
              ],
              temperature: 1,
              max_tokens: 512,
            }),
            signal: controller.signal,
          });

          clearTimeout(timeoutId);

          if (!response.ok) {
            const errorText = await response.text();
            if (i < retries && response.status >= 500) {
              await new Promise(r => setTimeout(r, 1500 * (i + 1)));
              continue;
            }
            throw new Error(`Kimi API error: ${response.status} ${errorText}`);
          }

          return response;
        } catch (err) {
          if (i === retries) throw err;
          console.log(`Attempt ${i + 1} failed: ${err.message}, retrying...`);
          await new Promise(r => setTimeout(r, 1500 * (i + 1)));
        }
      }
    };

    const response = await fetchWithRetry();
    const data = await response.json();

    const content = data.choices?.[0]?.message?.content ?? '';
    if (!content) {
      return new Response(JSON.stringify({ error: 'Empty response from Kimi', details: data }), {
        status: 502,
        headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }
      });
    }

    // 提取 JSON（防止偶尔有多余文字）
    const jsonMatch = content.match(/\{[\s\S]*\}/);
    if (!jsonMatch) {
      return new Response(JSON.stringify({ error: 'No JSON in Kimi response', content }), {
        status: 502,
        headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }
      });
    }

    const priceData = JSON.parse(jsonMatch[0]);

    // 确保 price 是数字
    if (typeof priceData.price === 'string') {
      priceData.price = parseFloat(priceData.price.replace(/[^\d.]/g, ''));
    }

    return new Response(JSON.stringify(priceData), {
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
      }
    });

  } catch (error) {
    const isTimeout = error.name === 'AbortError' || error.message?.includes('timeout');
    return new Response(JSON.stringify({
      error: isTimeout ? '查询超时，请重试' : error.message,
    }), {
      status: isTimeout ? 504 : 500,
      headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }
    });
  }
}
