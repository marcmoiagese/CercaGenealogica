import { test, expect } from '@playwright/test';

test('la home carrega sense errors visibles', async ({ page }) => {
  await page.goto('/');

  await expect(page.locator('body')).toBeVisible();
  await expect(page.locator('body')).not.toContainText('panic');
  await expect(page.locator('body')).not.toContainText('runtime error');
  await expect(page.locator('body')).not.toContainText('Internal Server Error');
});