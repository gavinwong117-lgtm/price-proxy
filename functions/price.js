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
    const BOT_ID = 'bot-20260315092034-mbzgp';
    if (!API_KEY) {
      return new Response(JSON.stringify({ error: 'DOUBAO_API_KEY not set' }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' }
      });
    }

    // 带重试的 fetch 函数
    const fetchWithRetry = async (retries = 2) => {
      for (let i = 0; i <= retries; i++) {
        try {
          const controller = new AbortController();
          const timeoutId = setTimeout(() => controller.abort(), 8000); // 8秒超时

          const response = await fetch('https://ark.cn-beijing.volces.com/api/v3/bots/chat/completions', {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'Authorization': `Bearer ${API_KEY}`
            },
            body: JSON.stringify({
              model: BOT_ID,
              messages: [{ role: 'user', content: `资产类别：${category}，资产名称：${name}，请告诉我当前每单位的大概价格（人民币）。` }],
              temperature: 0.1
            }),
            signal: controller.signal
          });

          clearTimeout(timeoutId);

          if (!response.ok) {
            const errorText = await response.text();
            if (i < retries && response.status >= 500) {
              console.log(`Retry ${i+1} due to status ${response.status}`);
              await new Promise(resolve => setTimeout(resolve, 1000 * (i+1)));
              continue;
            }
            throw new Error(`Doubao Bot API error: ${response.status} ${errorText}`);
          }

          return response;
        } catch (err) {
          if (i === retries) throw err;
          if (err.name === 'AbortError') {
            console.log(`Attempt ${i+1} timeout, retrying...`);
          } else {
            console.log(`Attempt ${i+1} failed: ${err.message}, retrying...`);
          }
          await new Promise(resolve => setTimeout(resolve, 1000 * (i+1)));
        }
      }
    };

    const response = await fetchWithRetry();
    const data = await response.json();

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
      headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }
    });

  } catch (error) {
    if (error.name === 'AbortError' || error.message.includes('timeout')) {
      return new Response(JSON.stringify({ error: 'Request timeout, please try again' }), {
        status: 504,
        headers: { 'Content-Type': 'application/json' }
      });
    }
    return new Response(JSON.stringify({ error: error.message, stack: error.stack }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' }
    });
  }
}