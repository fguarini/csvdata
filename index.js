#!/usr/bin/env node

'use strict';


exports.checkFirstLine = function checkFirstLine (firstLine, opts) {
  const { delimiter, limit } = opts;
  let cols = firstLine.split(delimiter);
  if (cols.length === 1 && cols[0] === '') {
    return {
      status: 'File has problems',
      error: 'File appears to be empty!'
    };
  }
  cols.forEach(col => {
    if (col === '') {
      return {
        status: 'File has problems',
        error: 'The CSV header contains empty values'
      }
    }
  });

  if (limit) {
    const columnValues = [];
    limit.split(',').forEach(col => {
      let i = cols.indexOf(col);
      if (i === -1) {
        columnValues.push(col);
      }
    });
    return {
      status: 'File has problems',
      error: `The following columns values did not correpond to the schema asked for the CSV headers\n` +
          'Please provide valid column names (string format, comma separated): \n' +
          `${columnValues}`
    }
  }
};

exports.checkBody = function checkBody (rs, usrOpts) {
  let result = true;
  let count = 1;
  let missing = [];
  let emptyLines = opts.emptyLines === true ? [] : false;
  let emptyValues = opts.emptyValues === true ? [] : false;
  let duplicates;

  if (opts.duplicates === true) {
    // Duplicate checking is done through an array that for each column
    // contains an array with two objects: "memo" and "map".
    // "memo" is to check if the value already exists, and
    // "map" is where duplicates coordinates are actually stored.
    duplicates = [];
    if (limit) {
      limit.forEach(col => {
        duplicates[col] = [{},{}];
      });
    } else {
      for (let i = 0; i < hlen; i++) {
        duplicates[i] = [{},{}];
      }
    }
  }

  /* Internal functions */
  function checkEmptyValues (line, col, item) {
    if (line.length !== 1 && item === '') {
      result = false;
      log && emptyValues.push([count,col]);
    }
  }

  function checkDuplicates (col, item) {
    if (item !== '' && item !== undefined) {
      let memo = duplicates[col][0][item];
      if (memo === undefined) {
        duplicates[col][0][item] = count;
      } else {
        result = false;
        if (log) {
          let map = duplicates[col][1][item];
          map ? map.push(count) : (duplicates[col][1][item] = [memo, count]);
        }
      }
    }
  }

  function check (line) {
    // Check missing values and empty lines
    if (line.length !== hlen) {
      if (line.length === 1 && line[0] === '') {
        if (emptyLines) {
          result = false;
          log && emptyLines.push(count);
        }
      } else {
        result = false;
        log && missing.push(count);
      }
    }
    // Check empty or duplicate values
    if (emptyValues || duplicates) {
      if (limit) {
        for (let i = 0; i < limit.length; i++) {
          let col = limit[i];
          let item = line[col];
          emptyValues && checkEmptyValues(line, col, item);
          duplicates && checkDuplicates(col, item);
        }
      } else {
        for (let col = 0; col < hlen; col++) {
          let item = line[col];
          emptyValues && checkEmptyValues(line, col, item);
          duplicates && checkDuplicates(col, item);
        }
      }
    }
    count++;
  }
  /* End of Internal functions */


  // Stream processing (using functions previously defined)
  rs
    .pipe(through(check))
    .on('end', () => {
      const resultObject = {};
      if (result) {
        Object.assign(resultObject, {status: 'File looks ok'});
      } else {
        Object.assign(resultObject, {status: 'File has problems!'});

        if (missing && missing[0] !== undefined) {
          Object.assign(resultObject, {
            missingValues: {
              message: 'Missing value on line(s)',
              lines: missing
            } 
          });
        }

        if (emptyLines && emptyLines[0] !== undefined) {
          Object.assign(resultObject, {
            emptyLines: {
              message: 'Empty line(s)',
              lines: emptyLines
            }
          });
        }
        
        if (emptyValues && emptyValues[0] !== undefined) {
          const emptyValuesObject = {};
          const emptyValuesArray = [];
          for (let i = 0; i < emptyValues.length; i++) {
            let item = emptyValues[i];
            emptyValuesArray.push(`${item[0]} (${cols[item[1]]})`);
          }
          Object.assign(emptyValuesObject, {
            message: 'Empty value(s) on line(s)',
            values: emptyValuesArray
          });
          
          Object.assign(resultObject, {emptyValues: emptyValuesObject});
        }

        if (duplicates) {
          for (let col = 0; col < hlen; col++) {
            if (duplicates[col]) {
              let map = duplicates[col][1];
              let mapKeys = Object.keys(map);
              if (mapKeys.length !== 0) {
                const duplicatesObject = {};
                const duplicatesArray = [];
                Object.assign(duplicatesObject, {message: `Duplicate values for "${cols[col]}`});
                for (let i = 0; i < mapKeys.length; i++) {
                  let key = mapKeys[i];
                  duplicatesArray.push(`"${key}" on line: ${map[key].join(', ')}`);
                }
                Object.assign(duplicatesObject, {values: duplicatesArray});
                Object.assign(resultObject, {duplicates: duplicatesObject});
              }
            }
          }
        }
      }
      resolve(resultObject);
    })
};