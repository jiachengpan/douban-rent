const _ = require('lodash');
const crypto = require('crypto');
const redis = require('redis');
const url = require('url'); 
const HCCrawler = require('headless-chrome-crawler');
const RedisCache = require('headless-chrome-crawler/cache/redis');
const Tgfancy = require("tgfancy");

const cache = new RedisCache({ host: '127.0.0.1', port: 6379 });
const client = redis.createClient();

const { promisify } = require("util");
const client_get_async = promisify(client.get).bind(client);

const config = require('./config.js');

const telegram_chat_id = config.tg_chat_id;
const telegram = new Tgfancy(config.tg_key, {
  tgfancy: {
    textPaging: true,
    ratelimiting: {
      maxRetries: 10,
      timeout: 1000 * 60,
    },
  },
});

const urls = [
  'https://www.douban.com/group/496399/',
  'https://www.douban.com/group/599811/',
  'https://www.douban.com/group/580888/',
  'https://www.douban.com/group/492107/',
  'https://www.douban.com/group/642507/',
  'https://www.douban.com/group/597660/',
  'https://www.douban.com/group/513885/',
  'https://www.douban.com/group/TZhezu/',
  'https://www.douban.com/group/609867/',
  'https://www.douban.com/group/603560/',
  // 'https://www.douban.com/group/search?cat=1013&q=%E9%9C%B2%E5%8F%B0+%E5%94%90%E9%95%87', // 露台 唐镇
  // 'https://www.douban.com/group/search?cat=1013&q=%E9%9C%B2%E5%8F%B0+%E5%BC%A0%E6%B1%9F', // 露台 张江
  // 'https://www.douban.com/group/search?cat=1013&q=%E9%9C%B2%E5%8F%B0+%E5%B9%BF%E5%85%B0', // 露台 广兰
  // 'https://www.douban.com/group/search?cat=1013&q=%E9%98%81%E6%A5%BC+%E5%94%90%E9%95%87', // 阁楼 唐镇
  // 'https://www.douban.com/group/search?cat=1013&q=%E9%98%81%E6%A5%BC+%E5%B9%BF%E5%85%B0', // 阁楼 广兰
  // 'https://www.douban.com/group/search?cat=1013&q=%E9%98%81%E6%A5%BC+%E5%BC%A0%E6%B1%9F', // 阁楼 张江
];



(async () => {
  let error_403s = 0;
  const k_delay = 10 * 1000;

  const crawler = await HCCrawler.launch({
    evaluatePage: (() => ({
      topic_title: $(".tablecc").text().trim(),
      topic:  $(".article h1").text().trim(),
      user:   $(".article .from").text().trim(),
      text:   $(".article .rich-content").text().trim(),
      img:    $(".article .rich-content img").attr('src'),
    })),
    onSuccess: (async result => {
      console.log(result.response.url, result.response.status);
      console.log(result.result);

      if (result.response.status == 403) {
        error_403s += 1;
        if (error_403s > 5) {
          crawler.pause();
          setTimeout(() => {
            telegram.sendMessage(telegram_chat_id, `crawler resumed`);
            crawler.resume();
          }, 300 * 60 * 1000);
          telegram.sendMessage(telegram_chat_id, `crawler paused with ${error_403s} 403 errors`);
          return;
        }
      } else {
        error_403s = 0;
      }

      const record_key = 'douban_topic.' + crypto.createHash('md5')
        .update(result.result.topic + ' - ' + result.result.user).digest('hex');

      try {
        const ret = await client_get_async(record_key);
        if (ret === null) {
          console.log('new!');

          const text = [ 
              result.response.url,
              result.result.topic_title ? result.result.topic_title : result.result.topic,
              result.result.user,
              result.result.text,
          ].join('\n');

          if (result.result.img) {
            telegram.sendPhoto(telegram_chat_id,
              result.result.img.replace('.webp', '.jpg'), {
              caption: text,
            }).catch(console.error);
          } else {
            telegram.sendMessage(telegram_chat_id, text).catch(console.error);
          };

          client.set(record_key, Date.now());
        }
      } catch (err) {
        console.error(err);
      }

      const next_links = {};

      const intersect = _.intersection(result.links, urls);

      // skip next link if not of our intereseted group
      if (result.response.url.match(/topic/) && !intersect.length) return;

      for (const link of result.links) {
        if (link.match(/group\/topic/)) {
          const parsed = new URL(link);
          const next_link = 'https://' + (parsed.host + '/' + parsed.pathname).replace(/\/+/, '/');
          next_links[next_link] = 1;
        }
      }

      for (const link in next_links) {
        await crawler.queue({url: link, delay: k_delay});
      }
    }),
    persistCache: true,
    cache: cache,
    maxConcurrency: 1,
  });


  const queue_urls = async () => {
    const queue_size = await crawler.queueSize();

    if (queue_size > 50) {
      console.log('skip queueing');
      return;
    }

    console.log('queuing new requests');
    for (const url of urls) {
      crawler.queue({
        url: url,
        delay: k_delay,
        skipDuplicates: false,
        priority: 100,
      });
    }
  }

  const query_status = async () => {
    console.log('queue size:', await crawler.queueSize());
    console.log('pending queue size:', await crawler.pendingQueueSize());
  };

  queue_urls();
  setInterval(queue_urls, 5 * 60 * 1000);
  setInterval(query_status, 60 * 1000);

  const listen_event = (event_name) => {
    crawler.on(event_name, (e) => {
      if (e && e.url) {
        console.log(event_name, 'delay', e.delay, 'url', e.url);
      } else {
        console.log(event_name, e);
      }
    });
  };

  listen_event('requeststarted');
  // listen_event('requestskipped');
  // listen_event('requestfinished', );
  listen_event('requestretried');
  listen_event('requestfailed');
  listen_event('maxdepthreached');
  listen_event('maxrequestreached');
  listen_event('disconnected');
})();
