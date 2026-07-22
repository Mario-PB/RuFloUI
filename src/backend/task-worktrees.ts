/**
 * task-worktrees.ts — safe git-worktree provisioning for WRITE-mode tasks.
 *
 * Goals (from ACC-TASK-QUEUE-002-REPAIR):
 *   • Create a fresh git worktree per WRITE task before any Claude process
 *     is spawned. Source cwd is never touched.
 *   • Deterministic branch / path derived from taskId + repo identity so
 *     the same taskId in two different repos produces distinct paths.
 *   • Defense in depth: rejects path traversal, invalid refs, existing
 *     worktree / branch collisions and non-git source. Existing
 *     unregistered filesystem paths count as collisions.
 *   • Never auto-removes an existing worktree, never overwrites a foreign
 *     branch. Cancel/cleanup is operator-driven (out of scope here).
 *
 * Security:
 *   • Absolute RUFLO_TASK_WORKTREE_ROOT inside source repo is rejected.
 *   • Relative/traversal env roots are rejected (fail-closed).
 *   • Default root is a durable user-owned path under os.homedir().
 *   • Repository identity component isolates same task IDs across repos.
 *   • Symlink/realpath containment is enforced.
 *
 * No git plumbing inside business logic — all git calls go through
 * `execFile` with explicit `--` argument arrays. No shell concatenation.
 */

import { execFile } from 'child_process'
import { promisify } from 'util'
import path from 'path'
import fs from 'fs'
import crypto from 'crypto'
import os from 'os'

const execFileAsync = promisify(execFile)

/**
 * Resolve the on-disk worktree root.
 *
 * Rules:
 *   1. If env var is unset → durable user-owned default under homedir.
 *   2. If env var is relative OR contains traversal → fail-closed
 *      (return null; caller treats null as `invalid-root`).
 *   3. If env var is absolute and resolves to a path that is equal to or
 *      descendant of the source cwd (after canonical realpath on both
 *      sides) → fail-closed.
 *   4. Otherwise → env var wins.
 *
 * The default is `~/worktrees/ruflo-tasks` and is created on first use.
 */
export function resolveDefaultWorktreeRoot(sourceCwd: string): string | null {
  const env = process.env.RUFLO_TASK_WORKTREE_ROOT
  if (env === undefined || env === null || env === '') {
    return defaultUserRoot()
  }
  if (typeof env !== 'string') return null

  // Relative env roots are ambiguous and easy to abuse; reject fail-closed.
  if (!path.isAbsolute(env)) return null

  // Reject obvious traversal segments up front (belt + suspenders).
  if (/(^|[\\/])\.\.([\\/]|$)/.test(env)) return null

  let resolvedEnv: string
  try {
    resolvedEnv = fs.realpathSync.native ? fs.realpathSync.native(env) : fs.realpathSync(env)
  } catch {
    // The directory doesn't exist yet; resolve lexically and validate against
    // the canonicalized source cwd (which must exist).
    resolvedEnv = path.resolve(env)
  }

  let resolvedSource: string
  try {
    resolvedSource = fs.realpathSync.native ? fs.realpathSync.native(sourceCwd) : fs.realpathSync(sourceCwd)
  } catch {
    // sourceCwd is invalid; caller will surface this with a clearer message.
    return resolvedEnv
  }

  if (isWithinOrEqual(resolvedEnv, resolvedSource)) return null

  return resolvedEnv
}

/** Durable default: `~/worktrees/ruflo-tasks`. Created lazily. */
function defaultUserRoot(): string {
  const home = os.homedir() || os.tmpdir()
  return path.join(home, 'worktrees', 'ruflo-tasks')
}

/** True when `child` is the same as `parent` or a descendant of it (after canonical resolution). */
function isWithinOrEqual(child: string, parent: string): boolean {
  const c = path.resolve(child)
  const p = path.resolve(parent)
  if (c === p) return true
  const sep = path.sep
  return c.startsWith(p + sep)
}

export interface WorktreeInfo {
  worktreePath: string     // absolute path on disk
  branchName: string       // e.g. ruflo-task/<repoHash>-<taskId>
  baseCommit: string       // HEAD sha at creation time (locked at provision)
  createdAt: string        // ISO timestamp
  /** Identifier of the source repository (sha256 prefix of canonical cwd). */
  repoId: string
}

export interface WorktreeManager {
  /** Resolve which path + branch a given taskId should use. Pure — no FS calls. */
  plan(taskId: string, sourceCwd: string): WorktreeInfo
  /**
   * Provision the worktree for a WRITE task. Validates the source is a
   * git repo, no existing worktree / branch collides, and the path is
   * safe. Returns the WorktreeInfo that was actually created.
   *
   * Throws a typed `WorktreeError` so the dispatcher can fail-closed
   * (mark the task as `failed` instead of dispatching Claude).
   */
  provision(taskId: string, sourceCwd: string): Promise<WorktreeInfo>
  /** Inspect existing worktree + branch presence without mutating anything. */
  inspect(taskId: string, sourceCwd: string): Promise<{ worktreePath: string; branchName: string; exists: boolean }>
  /** Pure: derive branch/path for a taskId. Exposed for tests + dispatcher. */
  derive(taskId: string, sourceCwd: string): { branchName: string; worktreePath: string; repoId: string }
}

/**
 * Injected git runner signature. Used by tests to simulate exact git
 * exit codes (1 = branch absent, 128 = permission/ref error, 126 = IO).
 * Production wiring uses the real `execFile` via `defaultGitRunner`.
 */
export type GitRunner = (args: string[], cwd: string) => Promise<string>

/** Default production runner — spawns `git` via execFile. */
export const defaultGitRunner: GitRunner = async (args, cwd) => git(args, cwd)

export class WorktreeError extends Error {
  public readonly code: string
  constructor(code: string, message: string) {
    super(message)
    this.code = code
    this.name = 'WorktreeError'
  }
}

/** Exported for tests + advanced wiring. Production code should use getWorktreeManager(). */
export { DefaultWorktreeManager }

/** Sanitize a taskId into a safe git-ref suffix. */
function sanitizeTaskId(taskId: string): string {
  if (!taskId || typeof taskId !== 'string') {
    throw new WorktreeError('invalid-task-id', 'taskId must be a non-empty string')
  }
  if (taskId.length > 200) {
    throw new WorktreeError('invalid-task-id', `taskId too long: ${taskId.length}`)
  }
  // Branch names may contain letters, digits, dot, slash, dash, underscore.
  // Strip everything else to a single dash so we never produce `..` traversal.
  const cleaned = taskId.replace(/[^A-Za-z0-9._-]/g, '-')
  if (cleaned.includes('..') || cleaned.startsWith('-') || cleaned.endsWith('-') || /^[-.]+$/.test(cleaned)) {
    throw new WorktreeError('invalid-task-id', `Refusing to derive ref from taskId: ${taskId}`)
  }
  return cleaned
}

/**
 * Compute a short, stable repo identifier from the canonical source cwd.
 * Used to namespace branches / worktree paths so that two repos with the
 * same taskId never collide on the global worktree root.
 */
function computeRepoId(sourceCwd: string): string {
  let canonical: string
  try {
    canonical = fs.realpathSync.native ? fs.realpathSync.native(sourceCwd) : fs.realpathSync(sourceCwd)
  } catch {
    canonical = path.resolve(sourceCwd)
  }
  return crypto.createHash('sha256').update(canonical).digest('hex').slice(0, 12)
}

/** Reject any path that escapes the configured root via `..` or absolute segments. */
function assertSafePath(resolved: string, root: string): void {
  const normRoot = path.resolve(root) + path.sep
  const norm = path.resolve(resolved)
  if (!norm.startsWith(normRoot) && norm !== path.resolve(root)) {
    throw new WorktreeError('path-traversal', `Refusing to provision outside root: ${resolved}`)
  }
}

/** Run a git command and capture stdout/stderr; throw a typed WorktreeError on failure. */
async function git(args: string[], cwd: string): Promise<string> {
  try {
    const { stdout } = await execFileAsync('git', args, { cwd, maxBuffer: 32 * 1024 * 1024 })
    return stdout.toString()
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err)
    const stderr = (err as { stderr?: Buffer | string }).stderr
    const stderrText = stderr ? (Buffer.isBuffer(stderr) ? stderr.toString() : String(stderr)) : ''
    const tail = stderrText ? ` :: ${stderrText}`.slice(0, 400) : ''
    const code = (err as { code?: number | string }).code
    throw new WorktreeErrorWithMeta(
      'git-failed',
      `git ${args[0]} failed: ${msg}${tail}`,
      { exitCode: typeof code === 'number' ? code : undefined, stderr: stderrText, message: msg }
    )
  }
}

/** Like WorktreeError but carries raw stderr / exit code for diagnostic checks. */
class WorktreeErrorWithMeta extends WorktreeError {
  public readonly exitCode: number | undefined
  public readonly stderrText: string
  public readonly messageText: string
  constructor(code: string, message: string, meta: { exitCode?: number; stderr?: string; message?: string }) {
    super(code, message)
    this.exitCode = meta.exitCode
    this.stderrText = meta.stderr || ''
    this.messageText = meta.message || ''
  }
}

/**
 * Decide whether a `show-ref --verify` failure represents "branch
 * absent" vs. a real git error. Inspected stderr + message together.
 *
 * Canonical absence: exit=1 with empty stderr (when --quiet was passed),
 * or stderr containing the canonical "not a ref" / "not found" text.
 */
function isShowRefAbsentError(err: WorktreeError): boolean {
  const withMeta = err as WorktreeErrorWithMeta
  const stderr = (withMeta.stderrText || '').toLowerCase()
  const message = (withMeta.messageText || '').toLowerCase()
  if (stderr.includes('not a ref') || message.includes('not a ref')) return true
  if (stderr.includes('not found') || message.includes('not found')) return true
  if (stderr.includes('needed a single revision') || message.includes('needed a single revision')) return true
  // --quiet: branch absent → exit=1, no stderr. That is the canonical
  // absence signal. ANY other non-1 exit code is treated as a real
  // failure (fail-closed).
  if (withMeta.exitCode === 1 && !stderr) return true
  return false
}

/**
 * Verify a branch ref name against git's own rules BEFORE we mutate
 * anything. `git check-ref-format --branch` exits non-zero if the name
 * is invalid; we treat ANY non-zero exit as fail-closed (not as
 * "branch absent").
 */
async function validateBranchName(repoCwd: string, branchName: string, runner: GitRunner = defaultGitRunner): Promise<void> {
  try {
    await runner(['check-ref-format', '--branch', branchName], repoCwd)
  } catch (err) {
    if (err instanceof WorktreeError) {
      throw new WorktreeError('invalid-branch', `Refused branch name ${branchName}: ${err.message}`)
    }
    throw err
  }
}

/** Confirm cwd is a git working tree (has .git dir or worktree pointer). */
async function isGitRepo(cwd: string, runner: GitRunner = defaultGitRunner): Promise<boolean> {
  try {
    await runner(['rev-parse', '--is-inside-work-tree'], cwd)
    return true
  } catch {
    return false
  }
}

class DefaultWorktreeManager implements WorktreeManager {
  private readonly runner: GitRunner
  constructor(opts: { runner?: GitRunner } = {}) {
    this.runner = opts.runner || defaultGitRunner
  }
  derive(taskId: string, sourceCwd: string): { branchName: string; worktreePath: string; repoId: string } {
    const safe = sanitizeTaskId(taskId)
    const repoId = computeRepoId(sourceCwd)
    const branchName = `ruflo-task/${repoId}-${safe}`
    const root = resolveDefaultWorktreeRoot(sourceCwd)
    if (!root) {
      throw new WorktreeError('invalid-root', `RUFLO_TASK_WORKTREE_ROOT is invalid or resolves inside the source repo: ${sourceCwd}`)
    }
    // Path includes repo identity so two repos with same taskId stay distinct.
    const worktreePath = path.join(root, repoId, `${safe}.worktree`)
    return { branchName, worktreePath, repoId }
  }

  plan(taskId: string, sourceCwd: string): WorktreeInfo {
    const { branchName, worktreePath, repoId } = this.derive(taskId, sourceCwd)
    const root = resolveDefaultWorktreeRoot(sourceCwd)
    if (!root) {
      throw new WorktreeError('invalid-root', `RUFLO_TASK_WORKTREE_ROOT is invalid or resolves inside the source repo: ${sourceCwd}`)
    }
    assertSafePath(worktreePath, root)
    return {
      worktreePath,
      branchName,
      baseCommit: '', // populated by provision()
      createdAt: '',
      repoId,
    }
  }

  async inspect(taskId: string, sourceCwd: string): Promise<{ worktreePath: string; branchName: string; exists: boolean }> {
    const { branchName, worktreePath } = this.derive(taskId, sourceCwd)
    const root = resolveDefaultWorktreeRoot(sourceCwd)
    if (!root) {
      throw new WorktreeError('invalid-root', `RUFLO_TASK_WORKTREE_ROOT is invalid or resolves inside the source repo: ${sourceCwd}`)
    }
    assertSafePath(worktreePath, root)
    if (!await isGitRepo(sourceCwd, this.runner)) {
      // Inspection never mutates, so we surface a falsy "exists" rather
      // than throwing — caller can decide.
      return { branchName, worktreePath, exists: false }
    }
    // Check branch existence without touching the worktree.
    try {
      await this.runner(['show-ref', '--verify', '--quiet', `refs/heads/${branchName}`], sourceCwd)
      return { branchName, worktreePath, exists: true }
    } catch (err) {
      if (err instanceof WorktreeError && err.code === 'git-failed') {
        // Distinguish "branch absent" (exit 1) from "other error".
        // execFile surfaces non-zero exit as an Error; we treat any git
        // error here as "not present" only when stderr matches the
        // canonical "not a ref" message. Otherwise fail-closed.
        const msg = (err.message || '').toLowerCase()
        const stderrTail = msg
        if (stderrTail.includes('not a ref') || stderrTail.includes('not found') || stderrTail.includes('needed a single revision')) {
          return { branchName, worktreePath, exists: false }
        }
        // Unknown git failure — treat as fail-closed (don't claim absent).
        throw new WorktreeError('git-failed', `show-ref failed for ${branchName}: ${err.message}`)
      }
      return { branchName, worktreePath, exists: false }
    }
  }

  async provision(taskId: string, sourceCwd: string): Promise<WorktreeInfo> {
    const { branchName, worktreePath, repoId } = this.derive(taskId, sourceCwd)
    const root = resolveDefaultWorktreeRoot(sourceCwd)
    if (!root) {
      throw new WorktreeError('invalid-root', `RUFLO_TASK_WORKTREE_ROOT is invalid or resolves inside the source repo: ${sourceCwd}`)
    }

    if (!sourceCwd || typeof sourceCwd !== 'string') {
      throw new WorktreeError('invalid-cwd', 'sourceCwd must be a non-empty string')
    }
    if (!fs.existsSync(sourceCwd)) {
      throw new WorktreeError('invalid-cwd', `source cwd does not exist: ${sourceCwd}`)
    }
    if (!await isGitRepo(sourceCwd, this.runner)) {
      throw new WorktreeError('non-git-cwd', `source cwd is not a git repository: ${sourceCwd}`)
    }

    // Reject path traversal explicitly (defense in depth — derive() already filters).
    assertSafePath(worktreePath, root)

    // Validate branch ref name with git itself BEFORE any mutation. Any
    // non-zero exit (including "invalid name") is a hard fail.
    await validateBranchName(sourceCwd, branchName, this.runner)

    // Branch collision: refuse to overwrite. Operator must rename/delete
    // the branch manually before retrying this task.
    //
    // We use `show-ref --verify --quiet` and only treat exit=1 with the
    // canonical "not a ref" stderr as "absent". ANY OTHER git error is
    // fail-closed (NOT treated as "absent").
    let branchExists = false
    try {
      await this.runner(['show-ref', '--verify', '--quiet', `refs/heads/${branchName}`], sourceCwd)
      branchExists = true
    } catch (err) {
      if (err instanceof WorktreeError && err.code === 'git-failed') {
        if (!isShowRefAbsentError(err)) {
          throw new WorktreeError('git-failed', `show-ref failed for ${branchName}: ${err.message}`)
        }
        branchExists = false
      } else {
        throw err
      }
    }
    if (branchExists) {
      throw new WorktreeError('collision-branch', `Branch ${branchName} already exists; refusing to overwrite`)
    }

    // Reject an existing registered git worktree at that exact path.
    try {
      const list = await this.runner(['worktree', 'list', '--porcelain'], sourceCwd)
      const lines = list.split('\n').filter(l => l.startsWith('worktree '))
      const existing = lines.find(l => l.slice('worktree '.length).trim() === worktreePath)
      if (existing) {
        throw new WorktreeError('collision-worktree', `A worktree already exists at ${worktreePath}`)
      }
    } catch (err) {
      if (err instanceof WorktreeError) throw err
      // git worktree list failed — surface as workflow error.
      throw new WorktreeError('git-failed', `git worktree list failed: ${(err as Error).message}`)
    }

    // Existing filesystem path (even unregistered as a git worktree) is a collision.
    try {
      const st = fs.lstatSync(worktreePath)
      if (st) {
        throw new WorktreeError('collision-path', `Filesystem path already exists at ${worktreePath}`)
      }
    } catch (err) {
      if (err instanceof WorktreeError) throw err
      // ENOENT means no collision; any other error we re-throw.
      const e = err as NodeJS.ErrnoException
      if (e?.code && e.code !== 'ENOENT') {
        throw new WorktreeError('collision-path', `Cannot stat worktree path ${worktreePath}: ${e.message}`)
      }
    }

    // Ensure root + repo subdir exist.
    try {
      fs.mkdirSync(root, { recursive: true })
      fs.mkdirSync(path.dirname(worktreePath), { recursive: true })
    } catch (err) {
      throw new WorktreeError('mkdir-failed', `Cannot create worktree root: ${root} (${(err as Error).message})`)
    }

    // Lock the base commit BEFORE mutating. Subsequent state changes must
    // be tied to the SHA we captured here, not to a moving HEAD.
    let baseCommit = ''
    try {
      baseCommit = (await this.runner(['rev-parse', 'HEAD'], sourceCwd)).trim()
    } catch (err) {
      throw new WorktreeError('git-failed', `Cannot resolve HEAD: ${(err as Error).message}`)
    }

    try {
      await this.runner(['worktree', 'add', '-b', branchName, worktreePath, baseCommit], sourceCwd)
    } catch (err) {
      throw new WorktreeError('worktree-add-failed', `git worktree add failed: ${(err as Error).message}`)
    }

    // Sanity probe: did the branch actually land in the new worktree?
    try {
      const head = (await this.runner(['-C', worktreePath, 'rev-parse', '--abbrev-ref', 'HEAD'], sourceCwd)).trim()
      if (head !== branchName) {
        throw new WorktreeError('worktree-add-failed', `Worktree HEAD is ${head}, expected ${branchName}`)
      }
    } catch (err) {
      if (err instanceof WorktreeError) throw err
      throw new WorktreeError('worktree-add-failed', `Worktree verification failed: ${(err as Error).message}`)
    }

    return {
      worktreePath,
      branchName,
      baseCommit,
      createdAt: new Date().toISOString(),
      repoId,
    }
  }
}

let _default: WorktreeManager | null = null
export function getWorktreeManager(): WorktreeManager {
  if (!_default) _default = new DefaultWorktreeManager()
  return _default
}
export function resetWorktreeManager(): void {
  _default = null
}
