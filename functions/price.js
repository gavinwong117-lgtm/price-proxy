export async function onRequest(context) {
  try {
    // 只允许 POST
    if (context.request.method !== 'POST') {
      return new Response('Method not allowed', { status: 405 });
    }

    // 解析请求体
    const { category, name } = await context.request.json();
    if (!category || !name) {
      return new Response(JSON.stringify({ error: 'Missing category or name' }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' }
      });
    }

    // 从环境变量读取豆包 API 密钥
    const API_KEY = context.env.DOUBAO_API_KEY;
    if (!API_KEY) {
      return new Response(JSON.stringify({ error: 'DOUBAO_API_KEY not set' }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' }
      });
    }

    // 调用豆包 API
    const response = await fetch('https://ark.cn-beijing.volces.com/api/v3/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${API_KEY}`
      },
      body: JSON.stringify({
        model: 'doubao-seed-2-0-mini-260215',  // 正确的模型 ID
        messages: [
          {
            role: 'system',
            content: '你是一个金融资产价格助手。用户会告诉你资产类别和名称，你需要给出该资产的当前大概价格（人民币）。只返回JSON格式：{"price": 数字, "unit": "单位", "note": "简短说明", "confidence": "high/medium/low"}。不要返回任何其他文字，不要用markdown代码块包裹。价格必须是数字。'
          },
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
      return new Response(JSON.stringify({ error: `Doubao API error: ${response.status} ${errorText}` }), {
        status: 502,
        headers: { 'Content-Type': 'application/json' }
      });
    }

    const data = await response.json();

    // 检查豆包返回的数据结构
    if (!data.choices || !data.choices[0] || !data.choices[0].message) {
      return new Response(JSON.stringify({ error: 'Unexpected Doubao API response', details: data }), {
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

    // 返回给客户端
    return new Response(JSON.stringify(priceData), {
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*'
      }
    });

  } catch (error) {
    // 捕获任何未预期的错误
    return new Response(JSON.stringify({ error: error.message, stack: error.stack }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' }
    });
  }
}