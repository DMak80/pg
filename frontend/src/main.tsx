// Точка входа SPA (минимальная версия каркаса; заменяется полной в задаче 8).
import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <h1>AdminPanel</h1>
  </StrictMode>,
);
