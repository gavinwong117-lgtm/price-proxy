export async function onRequest(context) {
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

    const API_KEY = context.env.KIMI_API_KEY;
    if (!API_KEY) {
      return new Response(JSON.stringify({ error: 'KIMI_API_KEY not set' }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' }
      });
    }

    const systemPrompt = `你是资产估价助手。必须联网搜索获取最新价格。
只返回JSON，不要任何其他文字，不要markdown代码块：
{"price":数字,"unit":"单位","note":"一句话说明","confidence":"high/medium/low","category":"stock_cn/stock_us/fund/crypto/gold/realestate/bond/cash/car/other"}

category说明：stock_cn=A股, stock_us=美股, fund=基金, crypto=加密货币, gold=贵金属（含黄金/白银/铂金等）, realestate=房产, bond=债券, cash=现金存款, car=车辆, other=其他。
价格必须换算为人民币和中国常用计量单位（如克、股、平方米等），严禁返回美元或盎司为单位的价格。`;

    const userMessage = (!category || category === 'auto')
      ? name
      : `资产类别：${category}，资产名称：${name}`;

    const kimiCall = async (messages) => {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 25000);
      try {
        const res = await fetch('https://api.moonshot.cn/v1/chat/completions', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${API_KEY}`,
          },
          body: JSON.stringify({
            model: 'kimi-k2.5',
            messages,
            tools: [
              { type: 'builtin_function', function: { name: '$web_search' } }
            ],
            thinking: { type: 'disabled' }, // 关闭思考模式，避免 reasoning_content 问题
            temperature: 0.6,                  // Kimi k2.5 关闭 thinking 后要求 temperature=1
            max_tokens: 512,
          }),
          signal: controller.signal,
        });
        clearTimeout(timeoutId);
        return res;
      } catch (err) {
        clearTimeout(timeoutId);
        throw err;
      }
    };

    // 第一轮
    const messages = [
      { role: 'system', content: systemPrompt },
      { role: 'user', content: userMessage },
    ];

    let res = await kimiCall(messages);
    if (!res.ok) {
      const errText = await res.text();
      throw new Error(`Kimi API error: ${res.status} ${errText}`);
    }
    let data = await res.json();
    let choice = data.choices?.[0];

    // 触发了 web_search，需要第二轮
    if (choice?.finish_reason === 'tool_calls') {
      const assistantMsg = choice.message;
      const toolCall = assistantMsg.tool_calls?.[0];

      messages.push(assistantMsg);
      messages.push({
        role: 'tool',
        content: toolCall.function.arguments,
        tool_call_id: toolCall.id,
      });

      res = await kimiCall(messages);
      if (!res.ok) {
        const errText = await res.text();
        throw new Error(`Kimi API error (round 2): ${res.status} ${errText}`);
      }
      data = await res.json();
      choice = data.choices?.[0];
    }

    const content = choice?.message?.content ?? '';
    if (!content) {
      return new Response(JSON.stringify({ error: 'Empty response from Kimi', details: data }), {
        status: 502,
        headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }
      });
    }

    const jsonMatch = content.match(/\{[\s\S]*\}/);
    if (!jsonMatch) {
      return new Response(JSON.stringify({ error: 'No JSON in Kimi response', content }), {
        status: 502,
        headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }
      });
    }

    const priceData = JSON.parse(jsonMatch[0]);
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
