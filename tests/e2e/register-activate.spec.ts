import { test, expect } from '@playwright/test';

const mailpitURL = process.env.MAILPIT_URL || 'http://devstack.marc.cat:8025';
const suffix = Date.now();
const username = `jaume_${suffix}`;
const email = `jaume_${suffix}@devstack.com`;
const password = '123456';

async function waitForActivationEmail(request: any, email: string) {
  const query = `to:${email}`;

  for (let i = 0; i < 30; i++) {
    const res = await request.get(`${mailpitURL}/api/v1/search`, {
      params: {
        query,
        limit: '10',
      },
    });

    expect(res.ok()).toBeTruthy();

    const data = await res.json();
    const messages = data.messages || data.Messages || [];

    if (messages.length > 0) {
      return messages[0];
    }

    await new Promise(resolve => setTimeout(resolve, 1000));
  }

  throw new Error(`No ha arribat cap email d'activació per a ${email}`);
}

async function getMailpitMessage(request: any, id: string) {
  const res = await request.get(`${mailpitURL}/api/v1/message/${id}`);
  expect(res.ok()).toBeTruthy();
  return await res.json();
}

function extractActivationURL(message: any): string {
  const body = [
    message.HTML,
    message.Html,
    message.Text,
    message.Body,
    message.Message,
    message.TextBody,
    message.HTMLBody,
    message.Raw,
  ]
    .filter(Boolean)
    .join('\n');

  const match = body.match(/https?:\/\/[^\s"'<>]*\/activar\?token=[^\s"'<>]+/i);

  if (!match) {
    throw new Error(`No s'ha trobat cap URL /activar?token= dins l'email`);
  }

  return match[0]
    .replace(/&amp;/g, '&')
    .replace(/[),.;]+$/g, '');
}

test('registre: crea usuari, rep email i activa el compte', async ({ page, request }) => {

  await test.step('Obrir formulari de registre', async () => {
    await page.goto('/en/');

    await expect(page.locator('body')).not.toContainText('panic');
    await expect(page.locator('body')).not.toContainText('runtime error');
    await expect(page.locator('body')).not.toContainText('Internal Server Error');

    await page.getByRole('link', { name: /sign up/i }).click();

    await expect(page.getByRole('textbox', { name: 'Username' })).toBeVisible();
    await expect(page.getByRole('button', { name: /create account/i })).toBeVisible();
  });

  await test.step('Omplir formulari de registre', async () => {
    await page.getByRole('textbox', { name: 'Username' }).fill(username);
    await page.getByRole('textbox', { name: 'First name' }).fill('Jaume');
    await page.getByRole('textbox', { name: 'Last name' }).fill('Català Sumalla');
    await page.getByRole('textbox', { name: 'Birth date' }).fill('1940-10-15');
    await page.getByRole('textbox', { name: 'Email' }).fill(email);
    await page.getByRole('textbox', { name: 'Password', exact: true }).fill(password);
    await page.getByRole('textbox', { name: 'Confirm password' }).fill(password);
    await page.getByRole('textbox', { name: /5 \+ 3/i }).fill('8');
    await page.getByRole('checkbox', { name: /I accept the terms/i }).check();

    const createAccountButton = page.getByRole('button', { name: /create account/i });
    await expect(createAccountButton).toBeVisible();
    await createAccountButton.scrollIntoViewIfNeeded();
    await createAccountButton.click();
  });

  await test.step('Comprovar que el registre ha enviat email', async () => {
    await expect(page.locator('body')).not.toContainText('panic');
    await expect(page.locator('body')).not.toContainText('runtime error');
    await expect(page.locator('body')).not.toContainText('Internal Server Error');

    await expect(page.locator('body')).toContainText(/email|mail|activation|activ/i);
  });

  await test.step('Llegir email d’activació des de Mailpit i activar compte', async () => {
    const summary = await waitForActivationEmail(request, email);
    const id = summary.ID || summary.Id || summary.MessageID || summary.MessageId;

    expect(id).toBeTruthy();

    const message = await getMailpitMessage(request, id);

    const activationURL = extractActivationURL(message);

    await page.goto(activationURL);

    await expect(page.locator('body')).not.toContainText('panic');
    await expect(page.locator('body')).not.toContainText('runtime error');
    await expect(page.locator('body')).not.toContainText('Internal Server Error');
    await expect(page.locator('body')).toContainText(/activated|activat|verified|confirm|correct/i);
  });

  await test.step('Fer login amb el compte activat', async () => {
    await page.goto('/en/');

    await page.getByRole('link', { name: /log in|sign in/i }).click();

    await page
      .getByRole('textbox', { name: /username or email|username|email/i })
      .fill(username);

    await page
      .getByRole('textbox', { name: /password/i })
      .fill(password);

    await page
      .getByRole('textbox', { name: /how much is 5 \+ 3/i })
      .fill('8');

    await page
      .getByRole('checkbox', { name: /remember me|keep me signed in/i })
      .check();

    await page
      .getByRole('button', { name: /^log in$|^sign in$/i })
      .click();

    await expect(page.locator('body')).not.toContainText('panic');
    await expect(page.locator('body')).not.toContainText('runtime error');
    await expect(page.locator('body')).not.toContainText('Internal Server Error');

    await expect(page.locator('#botoMenu')).toBeVisible();

    await page.locator('#botoMenu').click();

    await expect(page.locator('body')).toContainText(/perfil|tanca sessió|sortir|logout|usuari/i);
  });

});