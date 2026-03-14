// api/price.js
export async function onRequest(context) {
  const { request } = context;
  
  if (request.method !== 'POST') {
    return new Response('Method not allowed', { status: 405 });
  }

  try {
    const { category, name } = await request.json();
    const API_KEY = context.env.DOUBAO_API_KEY;

    const response = await fetch('https://ark.cn-beijing.volces.com/api/v3/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${API_KEY}`
      },
      body: JSON.stringify({
        model: 'doubao-seed-2.0-mini-260215',
        messages: [
          {
            role: 'system',
            content: '你是一个金融资产价格助手。只返回JSON格式：{"price": 数字, "unit": "单位", "note": "简短说明", "confidence": "high/medium/low"}'
          },
          {
            role: 'user',
            content: `资产类别：${category}，资产名称：${name}，请告诉我当前每单位的大概价格（人民币）。`
          }
        ],
        temperature: 0.1
      })
    });

    const data = await response.json();
    const content = data.choices[0].message.content;
    const jsonMatch = content.match(/\{.*\}/s);
    
    if (!jsonMatch) throw new Error('无法解析价格数据');
    const priceData = JSON.parse(jsonMatch[0]);

    return new Response(JSON.stringify(priceData), {
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*'
      }
    });

  } catch (error) {
    return new Response(JSON.stringify({ error: error.message }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' }
    });
  }
}