'use strict';
const fs = require('node:fs');
const path = require('node:path');
const {chromium} = require('playwright');
(async () => {
  let browser;
  const out = path.join(__dirname, 'native-smoke-result.json');
  try {
    browser = await chromium.launch({headless:false, chromiumSandbox:true, executablePath:'/usr/bin/chromium'});
    const context = await browser.newContext({viewport:{width:1100,height:700}});
    const page = await context.newPage();
    await page.setContent('<!doctype html><title>insane-search headed verification</title><h1>Visible Linux Chromium</h1><p>Playwright headed mode with Chromium sandbox enabled.</p>');
    let screenshotError = ''; try {await page.screenshot({path:path.join(__dirname,'native-smoke.png')});} catch(e){screenshotError=e.message;}
    fs.writeFileSync(out, JSON.stringify({ok:true,version:browser.version(),headless:false,chromiumSandbox:true,display:!!process.env.DISPLAY,screenshotError,title:await page.title(),html:await page.content()}));
    await new Promise(r=>setTimeout(r,30000));
  } catch (e) {
    fs.writeFileSync(out, JSON.stringify({ok:false,error:e.message}));
    process.exitCode = 1;
  } finally {if(browser)await browser.close();}
})();
