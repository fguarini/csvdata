'use strict';

const fs = require('fs');
const streamifier = require('streamifier');

module.exports = (path, usrOpts) => {
  const opts = {
    encoding: 'utf8',
    lineEnding: '\n'
  };
  Object.assign(opts, usrOpts);
  return new Promise((resolve, reject) => {
    let rs;
    if (typeof path === 'string') {
      rs = fs.createReadStream(path, {encoding: opts.encoding});
    } else {
      rs = streamifier.createReadStream(path, {encoding: opts.encoding});
    }
    let acc = '';
    let pos = 0;
    let index;
    rs
      .on('data', chunk => {
        index = chunk.indexOf(opts.lineEnding);
        acc += chunk;
        if (index === -1) {
          pos += chunk.length;
        } else {
          pos += index;
          if (typeof path === 'string') {
            rs.close();
          } else {
            rs.destroy();
          }
        }
      })
      .on('close', () => resolve(acc.slice(acc.charCodeAt(0) === 0xFEFF ? 1 : 0, pos)))
      .on('error', err => reject(err));
  });
};
