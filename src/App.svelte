<script lang="ts">
  import { invoke, convertFileSrc } from "@tauri-apps/api/core";
  import { listen } from "@tauri-apps/api/event";
  import { confirm, open } from "@tauri-apps/plugin-dialog";
  import { onMount } from "svelte";

  type Paths = {
    dbPath: string;
    image: string;
  };

  type Status = {
    available: boolean;
    version?: string;
    image: string;
  };

  type Preview = {
    kind: string;
    mime: string;
    dataUrl?: string;
    fileUrl?: string;
    label: string;
  };

  type Result = {
    score?: number;
    path?: string;
    sourcePath?: string;
    recordType?: string;
    text?: string;
    raw: Record<string, unknown>;
    preview?: Preview;
  };

  type Progress = {
    jobId: string;
    batchIndex: number;
    batchCount: number;
    path: string;
    status: "running" | "done" | "error" | "cancelled";
    message: string;
    ingestCurrent?: number;
    ingestTotal?: number;
    ingestPath?: string;
  };

  type DatabaseStatus = {
    exists: boolean;
    sizeBytes: number;
    entryCount?: number;
    uniqueFileCount?: number;
    tableName: string;
    tablePath: string;
    message?: string;
  };

  let rootPath = "";
  let dbPath = "";
  let image = "localhost/wolfe-podman:latest";
  let query = "";
  let limit = 12;
  let device = "auto";
  let lowMemory = true;
  let translate = false;
  let batchBySubfolder = false;
  let ignores = ".git\nnode_modules\n.target\n.cache";
  let status: Status | null = null;
  let database: DatabaseStatus | null = null;
  let activeJob = "";
  let progress: Progress[] = [];
  let results: Result[] = [];
  let busy = false;
  let searching = false;
  let building = false;
  let refreshingDatabase = false;
  let deletingDatabase = false;
  let notice = "";
  let error = "";

  onMount(async () => {
    const defaults = await invoke<Paths>("default_paths");
    dbPath = defaults.dbPath;
    image = defaults.image;
    await refreshStatus();
    await refreshDatabaseStatus();
    listen<Progress>("wolfe://index-progress", (event) => {
      progress = upsertProgress(progress, event.payload);
    });
    listen<{ jobId: string }>("wolfe://index-finished", (event) => {
      if (event.payload.jobId === activeJob) {
        busy = false;
        activeJob = "";
        void refreshDatabaseStatus();
      }
    });
  });

  async function refreshStatus() {
    status = await invoke<Status>("podman_status", { image });
  }

  async function refreshDatabaseStatus() {
    if (!dbPath) return;
    refreshingDatabase = true;
    try {
      database = await invoke<DatabaseStatus>("database_status", { request: { dbPath } });
    } catch (err) {
      database = null;
      error = String(err);
    } finally {
      refreshingDatabase = false;
    }
  }

  async function deleteDatabase() {
    if (!dbPath || busy) return;
    const accepted = await confirm("Delete the Wolfe database at the selected path?", {
      title: "Delete database",
      kind: "warning",
      okLabel: "Delete",
      cancelLabel: "Cancel",
    });
    if (!accepted) return;

    deletingDatabase = true;
    error = "";
    notice = "";
    try {
      await invoke("delete_database", { request: { dbPath } });
      results = [];
      notice = "Database deleted.";
      await refreshDatabaseStatus();
    } catch (err) {
      error = String(err);
    } finally {
      deletingDatabase = false;
    }
  }

  async function chooseRoot() {
    const selected = await open({ directory: true, multiple: false });
    if (typeof selected === "string") rootPath = selected;
  }

  async function chooseDbParent() {
    const selected = await open({ directory: true, multiple: false });
    if (typeof selected === "string") {
      dbPath = `${selected.replace(/\/$/, "")}/wolfe.lance`;
      await refreshDatabaseStatus();
    }
  }

  async function buildImage() {
    building = true;
    error = "";
    notice = "Building Wolfe podman image from the wolfe-podman repository.";
    try {
      await invoke("build_wolfe_image", { image: "localhost/wolfe-podman:latest" });
      image = "localhost/wolfe-podman:latest";
      notice = "Image built as localhost/wolfe-podman:latest.";
      await refreshStatus();
    } catch (err) {
      error = String(err);
    } finally {
      building = false;
    }
  }

  async function startIndex() {
    error = "";
    notice = "";
    progress = [];
    busy = true;
    try {
      const job = await invoke<{ id: string; batches: string[] }>("start_index", {
        request: {
          rootPath,
          dbPath,
          image,
          device,
          lowMemory,
          translate,
          batchBySubfolder,
          ignores: ignores.split("\n").map((item) => item.trim()).filter(Boolean),
        },
      });
      activeJob = job.id;
      progress = job.batches.map((path, index) => ({
        jobId: job.id,
        batchIndex: index,
        batchCount: job.batches.length,
        path,
        status: "running",
        message: index === 0 ? "Queued" : "Waiting",
      }));
    } catch (err) {
      busy = false;
      error = String(err);
    }
  }

  async function cancelIndex() {
    if (!activeJob) return;
    await invoke("cancel_index", { jobId: activeJob });
    notice = "Cancellation requested. The current podman batch may finish before the job stops.";
  }

  async function runSearch() {
    if (!query.trim()) return;
    searching = true;
    error = "";
    try {
      results = await invoke<Result[]>("search", {
        request: { query, dbPath, image, limit, device },
      });
      await refreshDatabaseStatus();
    } catch (err) {
      error = String(err);
    } finally {
      searching = false;
    }
  }

  async function reveal(path?: string) {
    if (!path) return;
    await invoke("reveal_path", { path });
  }

  function mediaSrc(preview: Preview) {
    if (preview.dataUrl) return preview.dataUrl;
    if (preview.fileUrl) return convertFileSrc(preview.fileUrl);
    return "";
  }

  function upsertProgress(items: Progress[], next: Progress) {
    const copy = [...items];
    const index = copy.findIndex((item) => item.jobId === next.jobId && item.batchIndex === next.batchIndex);
    if (index >= 0) copy[index] = next;
    else copy.push(next);
    return copy.sort((a, b) => a.batchIndex - b.batchIndex);
  }

  function itemPercent(item: Progress) {
    if (!item.ingestCurrent || !item.ingestTotal) return 0;
    return Math.min(100, Math.round((item.ingestCurrent / item.ingestTotal) * 100));
  }

  function formatBytes(bytes?: number) {
    if (!bytes) return "0 B";
    const units = ["B", "KB", "MB", "GB", "TB"];
    let size = bytes;
    let unit = 0;
    while (size >= 1024 && unit < units.length - 1) {
      size /= 1024;
      unit += 1;
    }
    return `${size >= 10 || unit === 0 ? size.toFixed(0) : size.toFixed(1)} ${units[unit]}`;
  }

  $: completed = progress.filter((item) => item.status === "done").length;
  $: failed = progress.filter((item) => item.status === "error").length;
  $: percent = progress.length ? Math.round(((completed + failed) / progress.length) * 100) : 0;
</script>

<main class="shell">
  <section class="topbar">
    <div>
      <h1>Wolfe</h1>
      <p>Local multimodal semantic search through a portable podman runtime.</p>
    </div>
    <div class:bad={!status?.available} class="podman-pill">
      <span>{status?.available ? "Podman ready" : "Podman missing"}</span>
      <small>{status?.version ?? "Install or start podman"}</small>
    </div>
  </section>

  <section class="workspace">
    <aside class="controls">
      <div class="panel">
        <h2>Index</h2>
        <label>
          Folder
          <div class="path-row">
            <input bind:value={rootPath} placeholder="/path/to/files" />
            <button type="button" class="icon-button" on:click={chooseRoot} title="Choose folder">...</button>
          </div>
        </label>

        <label>
          Database
          <div class="path-row">
            <input bind:value={dbPath} />
            <button type="button" class="icon-button" on:click={chooseDbParent} title="Choose database folder">...</button>
          </div>
        </label>

        <label>
          Podman image
          <input bind:value={image} />
        </label>

        <div class="grid-two">
          <label>
            Device
            <select bind:value={device}>
              <option value="auto">Auto</option>
              <option value="cpu">CPU</option>
              <option value="cuda">CUDA</option>
              <option value="mps">MPS</option>
            </select>
          </label>
          <label>
            Results
            <input type="number" min="1" max="100" bind:value={limit} />
          </label>
        </div>

        <div class="toggles">
          <label><input type="checkbox" bind:checked={batchBySubfolder} /> Batch top-level subfolders</label>
          <label><input type="checkbox" bind:checked={lowMemory} /> Low memory</label>
          <label><input type="checkbox" bind:checked={translate} /> Translate speech</label>
        </div>

        <label>
          Ignore names or paths
          <textarea bind:value={ignores} rows="4"></textarea>
        </label>

        <div class="actions">
          <button type="button" class="primary" disabled={busy || !rootPath || !dbPath} on:click={startIndex}>
            {busy ? "Indexing" : "Start indexing"}
          </button>
          <button type="button" disabled={!busy} on:click={cancelIndex}>Stop</button>
        </div>
        <button type="button" class="secondary" disabled={building} on:click={buildImage}>
          {building ? "Building image" : "Build local image"}
        </button>
      </div>

      <div class="panel database-panel">
        <div class="panel-head">
          <h2>Database</h2>
          <button type="button" class="compact-button" disabled={refreshingDatabase || !dbPath} on:click={refreshDatabaseStatus}>
            {refreshingDatabase ? "..." : "Refresh"}
          </button>
        </div>
        <div class="stat-grid">
          <div>
            <small>Files</small>
            <strong>{database?.uniqueFileCount?.toLocaleString() ?? "0"}</strong>
          </div>
          <div>
            <small>Entries</small>
            <strong>{database?.entryCount?.toLocaleString() ?? "0"}</strong>
          </div>
          <div>
            <small>Size</small>
            <strong>{formatBytes(database?.sizeBytes)}</strong>
          </div>
        </div>
        <div class="database-path">
          <small>{database?.tableName ?? "wolfe"}</small>
          <span class:muted={!database?.exists}>{database?.tablePath ?? dbPath}</span>
        </div>
        <button type="button" class="danger" disabled={busy || deletingDatabase || !database?.exists} on:click={deleteDatabase}>
          {deletingDatabase ? "Deleting" : "Delete database"}
        </button>
        {#if database?.message}
          <small>{database.message}</small>
        {/if}
      </div>

      <div class="panel progress-panel">
        <h2>Progress</h2>
        <div class="meter"><span style={`width: ${percent}%`}></span></div>
        <p>{completed} complete, {failed} failed, {progress.length} total</p>
        <div class="batch-list">
          {#each progress as item}
            <article class={`batch ${item.status}`}>
              <strong>{item.batchIndex + 1}. {item.path.split("/").pop() || item.path}</strong>
              {#if item.ingestCurrent && item.ingestTotal}
                <div class="mini-meter"><span style={`width: ${itemPercent(item)}%`}></span></div>
                <small>{item.ingestCurrent}/{item.ingestTotal} files</small>
              {/if}
              <small>{item.status}: {item.message.slice(0, 180)}</small>
            </article>
          {/each}
        </div>
      </div>
    </aside>

    <section class="search-pane">
      <div class="search-card">
        <input bind:value={query} on:keydown={(event) => event.key === "Enter" && runSearch()} placeholder="Search for a scene, phrase, sound, object, or document idea" />
        <button type="button" class="primary" disabled={searching || !query || !dbPath} on:click={runSearch}>
          {searching ? "Searching" : "Search"}
        </button>
      </div>

      {#if notice}
        <div class="notice">{notice}</div>
      {/if}
      {#if error}
        <pre class="error">{error}</pre>
      {/if}

      <div class="results">
        {#each results as result}
          <article class="result">
            <div class="preview">
              {#if result.preview?.kind === "image"}
                <img src={mediaSrc(result.preview)} alt={result.preview.label} />
              {:else if result.preview?.kind === "audio"}
                <audio src={mediaSrc(result.preview)} controls></audio>
              {:else if result.preview?.kind === "video"}
                <video src={mediaSrc(result.preview)} controls preload="metadata">
                  <track kind="captions" />
                </video>
              {:else if result.preview?.kind === "document"}
                <button type="button" class="doc-preview" on:click={() => reveal(result.sourcePath)}>PDF</button>
              {:else}
                <span>{result.recordType ?? "file"}</span>
              {/if}
            </div>
            <div class="result-body">
              <div class="result-head">
                <button type="button" on:click={() => reveal(result.sourcePath)}>{result.sourcePath ?? result.path ?? "Unknown source"}</button>
                {#if result.score !== undefined}<small>{result.score.toFixed(4)}</small>{/if}
              </div>
              <p>{result.text ?? JSON.stringify(result.raw).slice(0, 420)}</p>
              <small>{result.recordType ?? result.preview?.kind ?? "record"}</small>
            </div>
          </article>
        {/each}
      </div>
    </section>
  </section>
</main>
