<script>
  export let value;
  export let title;
  export let fmt = 'num0';
  export let change = null;
</script>

<div class="kpi-card">
  <div class="kpi-title">{title}</div>
  <div class="kpi-value">
    {#if fmt === 'num0'}
      {Math.round(value).toLocaleString()}
    {:else if fmt === 'pct1'}
      {value.toFixed(1)}%
    {:else if fmt === 'usd0'}
      ${Math.round(value).toLocaleString()}
    {:else}
      {value}
    {/if}
  </div>
  {#if change !== null}
    <div class="kpi-change" class:positive={change > 0} class:negative={change < 0}>
      {change > 0 ? '+' : ''}{change.toFixed(1)}%
    </div>
  {/if}
</div>

<style>
  .kpi-card {
    background: #1e293b;
    border: 1px solid #334155;
    border-radius: 0.75rem;
    padding: 1.5rem;
    text-align: center;
  }

  .kpi-title {
    font-size: 0.875rem;
    color: #94a3b8;
    margin-bottom: 0.5rem;
  }

  .kpi-value {
    font-size: 2rem;
    font-weight: 700;
    color: #f1f5f9;
  }

  .kpi-change {
    font-size: 0.75rem;
    font-weight: 500;
    margin-top: 0.25rem;
    color: #94a3b8;
  }

  .kpi-change.positive {
    color: #34d399;
  }

  .kpi-change.negative {
    color: #f87171;
  }
</style>
