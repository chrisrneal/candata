import { PROVINCES, INDICATOR_IDS } from '@candata/shared';

export default function Home() {
  return (
    <main>
      <h1>candata</h1>
      <p>Canadian Data Intelligence Platform</p>
      <section>
        <h2>Coverage</h2>
        <p>{Object.keys(PROVINCES).length} provinces and territories</p>
        <p>{INDICATOR_IDS.length} economic indicators</p>
      </section>
    </main>
  );
}
