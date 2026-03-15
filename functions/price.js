export async function onRequest(context) {
  try {
    if (context.request.method !== 'POST') {
      return new Response('Method not allowed', { status: 405 });
    }

    const { category, name } = await context.request.json();
    if (!category || !name) {
      return new Response(JSON.stringify({ error: 'Missing category or name' }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' }
      });
    }

    const API_KEY = context.env.DOUBAO_API_KEY;
    const BOT_ID = 'bot-20260315092034-mbzgp'; // 你的应用 ID
    if (!API_KEY) {
      return new Response(JSON.stringify({ error: 'DOUBAO_API_KEY not set' }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' }
      });
    }

    // 调用豆包零代码应用 API
    const response = await fetch('https://ark.cn-beijing.volces.com/api/v3/bots/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${API_KEY}`
      },
      body: JSON.stringify({
        model: BOT_ID, // 使用应用 ID
        messages: [
          {
            role: 'user',
            content: `资产类别：${category}，资产名称：${name}，请告诉我当前每单位的大概价格（人民币）。`
          }
        ],
        temperature: 0.1
      })
    });

    if (!response.ok) {
      const errorText = await response.text();
      return new Response(JSON.stringify({ error: `Doubao Bot API error: ${response.status} ${errorText}` }), {
        status: 502,
        headers: { 'Content-Type': 'application/json' }
      });
    }

    const data = await response.json();

    // 检查返回的数据结构（Bot 接口返回格式可能略有不同，但通常与 Chat 一致）
    if (!data.choices || !data.choices[0] || !data.choices[0].message) {
      return new Response(JSON.stringify({ error: 'Unexpected Doubao Bot API response', details: data }), {
        status: 502,
        headers: { 'Content-Type': 'application/json' }
      });
    }

    const content = data.choices[0].message.content;
    const jsonMatch = content.match(/\{.*\}/s);
    if (!jsonMatch) {
      return new Response(JSON.stringify({ error: 'No JSON found in Doubao response', content }), {
        status: 502,
        headers: { 'Content-Type': 'application/json' }
      });
    }

    const priceData = JSON.parse(jsonMatch[0]);

    return new Response(JSON.stringify(priceData), {
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*'
      }
    });

  } catch (error) {
    return new Response(JSON.stringify({ error: error.message, stack: error.stack }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' }
    });
  }
}