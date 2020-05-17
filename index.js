require('dotenv').config();

const knex = require('knex')(require('./knexfile'));
const fs = require('fs');
const twit = require('twit');
const axios = require('axios');
const path = require('path');
const headers = require('./headers');

const upsert = async (params) => {
  const {table, object, constraint} = params;
  const insert = knex(table).insert(object);
  const update = knex.queryBuilder().update(object);
	const query = await knex.raw(`? ON CONFLICT ${constraint} DO ? returning *`, [insert, update]);
	return query;
};

const Twitter = new twit({
  consumer_key: process.env.TWITTER_CONSUMER_KEY,
  consumer_secret: process.env.TWITTER_CONSUMER_SECRET,
  access_token: process.env.TWITTER_ACCESS_TOKEN,
  access_token_secret: process.env.TWITTER_ACCESS_TOKEN_SECRET,
  timeout_ms: 60*1000,
});

const transformer = array => array.map(each => ({
  post_id: each.video.id,
  video_url: each.video.downloadAddr,
  description: each.desc,
  author_id: each.author.id,
  posted_at: each.createTime,
}));

const getPosts = async () => {
  const members = await knex('members');

  const asyncX = members.map(async each => {
    const response = await axios.get(each.url, { headers })

    if (!response.data.items) return false;

    const data = transformer(response.data.items);
    data.reverse();

    const asyncY = data.map(async each => {
      await upsert({
        table: 'activities',
        object: each,
        constraint: '(post_id)'
      });
    });

    await Promise.all(asyncY);
  });

  await Promise.all(asyncX);
};

const downloadFile = async (url, fileName) => {  
	const filePath = path.resolve('tmp', fileName);
  const writer = fs.createWriteStream(filePath)

  const response = await axios({
    url,
    method: 'GET',
    responseType: 'stream',
  });

  response.data.pipe(writer);

  return new Promise((resolve, reject) => {
    writer.on('finish', resolve)
    writer.on('error', reject)
  });
};

const postMediaChunked = async (filePath) => new Promise((resolve, reject) => {
	Twitter.postMediaChunked({ file_path: filePath }, function (err, data) {
		if (err) reject(err)
		else {
      setTimeout(async () => {
				resolve(data.media_id_string)
			}, 5000);
		}
	});
});

const publish = async () => {
  try {
    const media = (await knex('activities')
      .join('members', 'activities.author_id', 'members.user_id')
      .where('is_published', false)
      .orderBy('posted_at', 'ASC')
      .limit(1))[0];

    console.log(media);

    if (media) {
      const fileName = 'media.mp4';
      await downloadFile(media.video_url, fileName);

      const filePath = path.join('tmp', fileName);

      const twitterMedia = await postMediaChunked(filePath);

      console.log(twitterMedia);
      
      let status = `Post from ${media.name}` + (media.description && media.description !== '' ? `: ${media.description}` : '');

      status = status.replace(/#/g, '#.');

      await Twitter.post('statuses/update', {
        status,
        media_ids: [twitterMedia],
      });
      
      await knex('activities')
        .update({is_published: true})
        .where('post_id', media.post_id);
    }

    setTimeout(async function() { await publish(); }, 30000);
  } catch (error) {
    console.log(error);
    await publish();
  }
};

publish();
getPosts();

setInterval(() => {
  getPosts();
}, 1000*60*5);
