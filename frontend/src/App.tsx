import { FormEvent, useEffect, useMemo, useRef, useState } from 'react'
import { apiFetch } from './lib/api'

type MailProvider = 'luckmail' | 'tempmail_lol' | 'outlook_local'
type Executor = 'protocol' | 'headless' | 'headed'
type SettingsTab = 'base' | 'mail' | 'uploads' | 'outlook'
type OutlookDeleteScope = 'all' | 'taken'
type ProxyDeleteScope = 'all'
type UploadTarget = 'cpa' | 'sub2api' | 'codexproxy'
type AutoUploadTarget = 'none' | 'cpa' | 'sub2api' | 'codexproxy' | 'both' | 'all'
type DeleteDialogState = {
  items: AccountItem[]
} | null

type TaskAttemptRef = {
  task_id: string
  attempt_index: number
}

type FormState = {
  count: number
  concurrency: number
  register_delay_seconds: number
  proxy: string
  use_proxy: boolean
  executor_type: Executor
  mail_provider: MailProvider
  luckmail_base_url: string
  luckmail_api_key: string
  luckmail_email_type: string
  luckmail_domain: string
  tempmail_api_base: string
  cpa_api_url: string
  cpa_api_key: string
  sub2api_api_url: string
  sub2api_api_key: string
  sub2api_group_ids: string
  codexproxy_api_url: string
  codexproxy_admin_key: string
  codexproxy_proxy_url: string
  auto_upload_target: AutoUploadTarget
  inbound_upload_auth_token: string
  inbound_upload_debug_logging: boolean
}

type AccountItem = {
  id?: number
  task_id?: string
  action_task_id?: string
  action_attempt_index?: number
  task_ids?: string[]
  task_refs?: TaskAttemptRef[]
  attempt_index: number
  email: string
  label: string
  status: string
  error?: string
  logs: string[]
  created_at: number
  updated_at: number
  flow_status?: string
  failure_stage?: string
  failure_stage_label?: string
  failure_origin?: string
  failure_detail?: string
  retry_supported?: boolean
}

type TaskSnapshot = {
  id: string
  status: string
  is_active?: boolean
  source?: string
  meta?: Record<string, unknown>
  request?: Record<string, unknown>
  progress: string
  success?: number
  failed?: number
  skipped?: number
  errors?: string[]
  summary?: Record<string, unknown>
  accounts?: AccountItem[]
}

type HistoryListResponse = {
  total: number
  items: Array<{
    id: string
    status: string
    created_at?: number
    updated_at?: number
    request?: Record<string, unknown>
  }>
}

type OutlookPoolItem = {
  id: number
  email: string
  enabled: boolean
  has_oauth: boolean
  created_at: number
  updated_at: number
  last_used: number
}

type OutlookPoolSummary = {
  total: number
  enabled: number
  disabled: number
  with_oauth: number
  items: OutlookPoolItem[]
}

type LuckMailTokenPoolItem = {
  id: number
  email: string
  token: string
  enabled: boolean
  created_at: number
  updated_at: number
  last_used: number
}

type LuckMailTokenPoolSummary = {
  total: number
  enabled: number
  disabled: number
  items: LuckMailTokenPoolItem[]
}

type LuckMailTokenImportResult = {
  total: number
  success: number
  updated: number
  failed: number
  accounts: Array<{
    id: number
    email: string
    enabled: boolean
    status: string
  }>
  errors: string[]
  summary: LuckMailTokenPoolSummary
}

type OutlookImportResult = {
  total: number
  success: number
  updated: number
  failed: number
  accounts: Array<{
    id: number
    email: string
    enabled: boolean
    has_oauth: boolean
    status: string
  }>
  errors: string[]
  summary: OutlookPoolSummary
}

type ProxyPoolItem = {
  id: number
  proxy_url: string
  enabled: boolean
  success_count: number
  failure_count: number
  last_checked_at: number
  last_check_status: string
  last_check_message: string
  last_ip: string
  last_country: string
  last_used_at: number
  created_at: number
  updated_at: number
}

type ProxyPoolSummary = {
  total: number
  enabled: number
  disabled: number
  healthy: number
  unhealthy: number
  success_count: number
  failure_count: number
  items: ProxyPoolItem[]
}

type ProxyImportResult = {
  total: number
  success: number
  updated: number
  failed: number
  items: Array<{
    id: number
    proxy_url: string
    enabled: boolean
    status: string
  }>
  errors: string[]
  summary: ProxyPoolSummary
}

const defaultForm: FormState = {
  count: 1,
  concurrency: 1,
  register_delay_seconds: 0,
  proxy: '',
  use_proxy: true,
  executor_type: 'protocol',
  mail_provider: 'luckmail',
  luckmail_base_url: 'https://mails.luckyous.com/',
  luckmail_api_key: '',
  luckmail_email_type: '',
  luckmail_domain: '',
  tempmail_api_base: 'https://api.tempmail.lol/v2',
  cpa_api_url: '',
  cpa_api_key: '',
  sub2api_api_url: '',
  sub2api_api_key: '',
  sub2api_group_ids: '2',
  codexproxy_api_url: '',
  codexproxy_admin_key: '',
  codexproxy_proxy_url: '',
  auto_upload_target: 'both',
  inbound_upload_auth_token: '',
  inbound_upload_debug_logging: false,
}

const settingsTabs: Array<{ key: SettingsTab; label: string }> = [
  { key: 'base', label: '基础设置' },
  { key: 'mail', label: '邮箱服务' },
  { key: 'uploads', label: '外部上传' },
  { key: 'outlook', label: '微软邮箱池' },
]
const accountPageSizeOptions = [20, 50, 100]

function StatCard({ label, value, tone = 'default' }: { label: string; value: string | number; tone?: string }) {
  return (
    <div className={`stat-card tone-${tone}`}>
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  )
}

function renderPoolEmail(email: string) {
  if (!email.includes('@')) return email
  const [name, domain] = email.split('@')
  if (name.length <= 3) return email
  return `${name.slice(0, 2)}***@${domain}`
}

function getAccountTitle(item: AccountItem) {
  return item.email || item.label || `第 ${item.attempt_index} 个账号`
}

function getStatusLabel(status: string) {
  const mapping: Record<string, string> = {
    pending: '等待中',
    running: '运行中',
    registering: '注册中',
    success: '成功',
    failed: '失败',
    stopped: '已停止',
    skipped: '已跳过',
    done: '已完成',
    idle: '空闲',
  }
  return mapping[status] || status
}

function getStageLabel(stage: string) {
  const mapping: Record<string, string> = {
    create_email: '创建邮箱',
    authorize_continue: '进入授权',
    otp: '邮箱验证码',
    about_you: '资料提交',
    workspace_select: '工作区选择',
    token_exchange: '令牌获取',
    oauth_login: 'OAuth 登录',
    register_flow: '注册流程',
    network_precheck: '网络检测',
  }
  return mapping[stage] || stage
}

function formatAccountLog(line: string) {
  const raw = String(line || '').trim()
  if (!raw) return ''

  const firstTimestamp = raw.match(/^\[(\d{2}:\d{2}:\d{2})\]\s*(.*)$/)
  const timePrefix = firstTimestamp ? `[${firstTimestamp[1]}]` : ''
  let text = firstTimestamp ? firstTimestamp[2] : raw

  text = text.replace(/^(?:\[\d{2}:\d{2}:\d{2}\]\s*)+/, '')
  text = text.replace(/^(?:\[[^\]]+\]\s*)+/, '')
  text = text.trim()

  return timePrefix ? `${timePrefix} ${text}`.trim() : text
}

function getLogTone(line: string) {
  const text = String(line || '').toLowerCase()
  if (text.includes('sentinel browser') && (text.includes('超时') || text.includes('回退'))) return 'warning'
  if (text.includes('获取成功') || text.includes('已通过') || text.includes('注册成功')) return 'success'
  if (text.includes('超时')) return 'warning'
  if (text.includes('失败') || text.includes('error')) return 'error'
  if (text.includes('成功')) return 'success'
  if (text.includes('验证码') || text.includes('verification code') || text.includes('otp')) return 'code'
  if (text.includes('停止') || text.includes('跳过')) return 'warning'
  if (text.includes('等待') || text.includes('延迟')) return 'info'
  return 'default'
}

function getInlineStatusClass(status: string) {
  const value = String(status || '').toLowerCase()
  if (value === 'success') return 'success'
  if (value === 'failed') return 'failed'
  if (value === 'pending') return 'warning'
  if (value === 'registering' || value === 'running') return 'running'
  if (value === 'stopped' || value === 'skipped') return 'warning'
  return 'default'
}

function isActiveAccountStatus(status: string) {
  return ['registering', 'running'].includes(String(status || '').toLowerCase())
}

function canStopAccount(item: AccountItem) {
  const status = String(item.status || '').toLowerCase()
  return ['pending', 'registering', 'running'].includes(status) && Boolean(item.action_task_id || item.task_id)
}

function getAccountStatusRank(status: string) {
  const value = String(status || '').toLowerCase()
  if (value === 'registering' || value === 'running') return 0
  if (value === 'pending') return 1
  return 2
}

function isTerminalAccountStatus(status: string) {
  return ['success', 'failed', 'stopped', 'skipped'].includes(String(status || '').toLowerCase())
}

function getActiveFlowLabel(item: AccountItem) {
  if (!isActiveAccountStatus(item.status)) return getStatusLabel(item.status)

  const logs = Array.isArray(item.logs) ? item.logs : []
  for (let index = logs.length - 1; index >= 0; index -= 1) {
    const raw = String(logs[index] || '').trim()
    if (!raw) continue

    const stageMatch = raw.match(/\[stage=([^\]]+)\]/)
    if (stageMatch?.[1]) {
      return getStageLabel(stageMatch[1].trim())
    }

    const formatted = formatAccountLog(raw)
    if (!formatted) continue

    if (formatted.includes('正在等待邮箱') || formatted.includes('等待邮箱验证码')) return '等待邮箱验证码'
    if (formatted.includes('验证 OTP') || formatted.includes('verify email otp') || formatted.includes('尝试 OTP')) return '验证邮箱验证码'
    if (formatted.includes('创建邮箱')) return '创建邮箱'
    if (formatted.includes('about_you') || formatted.includes('提交姓名和生日') || formatted.includes('提交资料')) return '资料提交'
    if (formatted.includes('工作区') || formatted.includes('workspace')) return '工作区选择'
    if (formatted.includes('Bootstrap OAuth session') || formatted.includes('/oauth/authorize')) return '初始化 OAuth'
    if (formatted.includes('authorize_continue') || formatted.includes('进入授权') || formatted.includes('Authorize')) return '进入授权'
    if (formatted.includes('passwordless')) return 'Passwordless 验证'
    if (formatted.includes('邮箱验证码') || formatted.includes('email-verification') || formatted.includes('email_otp')) return '邮箱验证码'
    if (formatted.includes('注册状态推进') || formatted.includes('注册状态起点') || formatted.includes('注册用户')) return '注册流程'
  }

  return '注册中'
}

export default function App() {
  const [form, setForm] = useState<FormState>(defaultForm)
  const [activeTask, setActiveTask] = useState<TaskSnapshot | null>(null)
  const [taskSnapshots, setTaskSnapshots] = useState<TaskSnapshot[]>([])
  const [outlookSummary, setOutlookSummary] = useState<OutlookPoolSummary | null>(null)
  const [luckmailTokenSummary, setLuckmailTokenSummary] = useState<LuckMailTokenPoolSummary | null>(null)
  const [proxySummary, setProxySummary] = useState<ProxyPoolSummary | null>(null)
  const [outlookImportText, setOutlookImportText] = useState('')
  const [luckmailTokenImportText, setLuckmailTokenImportText] = useState('')
  const [proxyImportText, setProxyImportText] = useState('')
  const [outlookImportResult, setOutlookImportResult] = useState<OutlookImportResult | null>(null)
  const [luckmailTokenImportResult, setLuckmailTokenImportResult] = useState<LuckMailTokenImportResult | null>(null)
  const [proxyImportResult, setProxyImportResult] = useState<ProxyImportResult | null>(null)
  const [expandedAccounts, setExpandedAccounts] = useState<string[]>([])
  const [selectedAccounts, setSelectedAccounts] = useState<string[]>([])
  const [showSettings, setShowSettings] = useState(false)
  const [settingsTab, setSettingsTab] = useState<SettingsTab>('base')
  const [loading, setLoading] = useState(true)
  const [starting, setStarting] = useState(false)
  const [saving, setSaving] = useState(false)
  const [importingOutlook, setImportingOutlook] = useState(false)
  const [importingLuckmailToken, setImportingLuckmailToken] = useState(false)
  const [importingProxy, setImportingProxy] = useState(false)
  const [deletingOutlookId, setDeletingOutlookId] = useState<number | null>(null)
  const [deletingOutlookScope, setDeletingOutlookScope] = useState<OutlookDeleteScope | null>(null)
  const [deletingLuckmailTokenId, setDeletingLuckmailTokenId] = useState<number | null>(null)
  const [deletingLuckmailTokenScope, setDeletingLuckmailTokenScope] = useState<OutlookDeleteScope | null>(null)
  const [deletingProxyId, setDeletingProxyId] = useState<number | null>(null)
  const [deletingProxyScope, setDeletingProxyScope] = useState<ProxyDeleteScope | null>(null)
  const [testingProxies, setTestingProxies] = useState(false)
  const [testingProxyId, setTestingProxyId] = useState<number | null>(null)
  const [retryingResultId, setRetryingResultId] = useState<number | null>(null)
  const [deletingAccounts, setDeletingAccounts] = useState(false)
  const [exportingZip, setExportingZip] = useState(false)
  const [uploadingTarget, setUploadingTarget] = useState<UploadTarget | null>(null)
  const [stoppingAccountKeys, setStoppingAccountKeys] = useState<string[]>([])
  const [accountPage, setAccountPage] = useState(1)
  const [accountPageSize, setAccountPageSize] = useState(20)
  const [deleteDialog, setDeleteDialog] = useState<DeleteDialogState>(null)
  const [error, setError] = useState('')
  const eventSourceRef = useRef<EventSource | null>(null)
  const accountLogRefs = useRef<Record<string, HTMLDivElement | null>>({})
  const refreshTimerRef = useRef<number | null>(null)
  const refreshInFlightRef = useRef(false)

  const isRunning = activeTask ? Boolean(activeTask.is_active) && !['done', 'failed', 'stopped'].includes(activeTask.status) : false

  const sortedAccounts = useMemo(() => {
    const merged = new Map<string, AccountItem>()
    const taskMap = new Map(taskSnapshots.map((item) => [item.id, item]))
    const resultOriginMap = new Map<number, { taskId: string; attemptIndex: number }>()

    for (const task of taskSnapshots) {
      for (const account of task.accounts || []) {
        if (typeof account.id === 'number') {
          resultOriginMap.set(account.id, {
            taskId: task.id,
            attemptIndex: Number(account.attempt_index || 0),
          })
        }
      }
    }

    function resolveBase(task: TaskSnapshot, account: AccountItem) {
      let baseTaskId = task.id
      let baseAttemptIndex = Number(account.attempt_index || 0)
      let currentTask: TaskSnapshot | undefined = task
      const visited = new Set<string>()

      while (currentTask && !visited.has(currentTask.id)) {
        visited.add(currentTask.id)
        const meta = currentTask.meta || {}
        const retryFromTaskId = String(meta.retry_from_task_id || '').trim()
        const retryFromAttemptIndex = Number(meta.retry_from_attempt_index || 0)
        const retryFromResultId = Number(meta.retry_from_result_id || 0)

        if (!retryFromTaskId) break

        baseTaskId = retryFromTaskId
        if (retryFromAttemptIndex > 0) {
          baseAttemptIndex = retryFromAttemptIndex
        } else if (retryFromResultId > 0) {
          const origin = resultOriginMap.get(retryFromResultId)
          if (origin) {
            baseTaskId = origin.taskId || baseTaskId
            if (origin.attemptIndex > 0) baseAttemptIndex = origin.attemptIndex
          }
        }
        currentTask = taskMap.get(retryFromTaskId)
      }

      return { baseTaskId, baseAttemptIndex }
    }

    for (const [taskIndex, task] of taskSnapshots.entries()) {
      for (const account of task.accounts || []) {
        const { baseTaskId, baseAttemptIndex } = resolveBase(task, account)
        const key = `${baseTaskId}:${baseAttemptIndex}`
        const normalized: AccountItem = {
          ...account,
          task_id: baseTaskId,
          action_task_id: task.id,
          action_attempt_index: Number(account.attempt_index || 0),
          task_ids: [task.id],
          task_refs: [{ task_id: task.id, attempt_index: Number(account.attempt_index || 0) }],
          attempt_index: baseAttemptIndex,
          created_at: Number(account.created_at || 0),
          updated_at: Number(account.updated_at || 0) || (taskSnapshots.length - taskIndex),
        }

        const current = merged.get(key)
        if (!current) {
          merged.set(key, { ...normalized, logs: [...normalized.logs] })
          continue
        }

        const currentLogs = Array.isArray(current.logs) ? current.logs : []
        const nextLogs = Array.isArray(normalized.logs) ? normalized.logs : []
        const taskIds = Array.from(new Set([...(current.task_ids || []), ...(normalized.task_ids || [])]))
        const taskRefs = new Map<string, TaskAttemptRef>()
        for (const ref of [...(current.task_refs || []), ...(normalized.task_refs || [])]) {
          const refTaskId = String(ref?.task_id || '').trim()
          const refAttemptIndex = Number(ref?.attempt_index || 0)
          if (!refTaskId || refAttemptIndex <= 0) continue
          taskRefs.set(`${refTaskId}:${refAttemptIndex}`, { task_id: refTaskId, attempt_index: refAttemptIndex })
        }

        const shouldReplace =
          ['pending', 'registering', 'running'].includes(normalized.status) ||
          (normalized.updated_at || 0) >= (current.updated_at || 0)

        merged.set(
          key,
          shouldReplace
            ? {
                ...current,
                ...normalized,
                logs: [...nextLogs],
                task_id: baseTaskId,
                action_task_id: normalized.action_task_id,
                action_attempt_index: normalized.action_attempt_index,
                task_ids: taskIds,
                task_refs: Array.from(taskRefs.values()),
                attempt_index: baseAttemptIndex,
              }
            : {
                ...current,
                logs: [...currentLogs],
                task_ids: taskIds,
                task_refs: Array.from(taskRefs.values()),
                retry_supported: Boolean(current.retry_supported || normalized.retry_supported),
                updated_at: Math.max(Number(current.updated_at || 0), Number(normalized.updated_at || 0)),
              },
        )
      }
    }

    const items = Array.from(merged.values())
    items.sort((a, b) => {
      const rankDiff = getAccountStatusRank(a.status) - getAccountStatusRank(b.status)
      if (rankDiff !== 0) return rankDiff

      if (getAccountStatusRank(a.status) <= 1 && getAccountStatusRank(b.status) <= 1) {
        const createdDiff = Number(b.created_at || 0) - Number(a.created_at || 0)
        if (createdDiff !== 0) return createdDiff
        return a.attempt_index - b.attempt_index
      }

      const updatedDiff = Number(b.updated_at || 0) - Number(a.updated_at || 0)
      if (updatedDiff !== 0) return updatedDiff
      const createdDiff = Number(b.created_at || 0) - Number(a.created_at || 0)
      if (createdDiff !== 0) return createdDiff
      return b.attempt_index - a.attempt_index
    })
    return items
  }, [taskSnapshots])

  const currentTaskAccounts = useMemo(() => {
    const activeTaskId = String(activeTask?.id || '').trim()
    if (!activeTaskId) return []
    return sortedAccounts.filter((item) => {
      const taskIds = Array.isArray(item.task_ids) ? item.task_ids.map((taskId) => String(taskId || '').trim()) : []
      return taskIds.includes(activeTaskId) || String(item.action_task_id || '').trim() === activeTaskId || String(item.task_id || '').trim() === activeTaskId
    })
  }, [activeTask?.id, sortedAccounts])

  const taskStats = useMemo(() => {
    const summary = activeTask?.summary || {}
    let accountSuccess = 0
    let accountFailed = 0
    let accountSkipped = 0
    let accountCompleted = 0

    for (const item of currentTaskAccounts) {
      const status = String(item.status || '').toLowerCase()
      if (status === 'success') {
        accountSuccess += 1
        accountCompleted += 1
      } else if (status === 'failed') {
        accountFailed += 1
        accountCompleted += 1
      } else if (status === 'stopped' || status === 'skipped') {
        accountSkipped += 1
        accountCompleted += 1
      }
    }

    const rawTotal = String(activeTask?.progress || '0/0').split('/')[1]
    const parsedTotal = Number(rawTotal || 0)
    const requestTotal = Number((activeTask?.request?.count as number | undefined) ?? 0)
    const total = Math.max(parsedTotal || 0, requestTotal || 0, currentTaskAccounts.length)

    return {
      success: Math.max(Number(activeTask?.success ?? summary.success ?? 0), accountSuccess),
      skipped: Math.max(Number(activeTask?.skipped ?? summary.skipped ?? 0), accountSkipped),
      failed: Math.max(Number(activeTask?.failed ?? (summary as Record<string, unknown>).failed ?? activeTask?.errors?.length ?? 0), accountFailed),
      completed: Math.max(accountCompleted, Math.min(total, accountSuccess + accountFailed + accountSkipped)),
      total,
    }
  }, [activeTask, currentTaskAccounts])

  const totalAccountPages = useMemo(
    () => Math.max(1, Math.ceil(sortedAccounts.length / accountPageSize)),
    [sortedAccounts.length, accountPageSize],
  )

  const pagedAccounts = useMemo(() => {
    const start = (accountPage - 1) * accountPageSize
    return sortedAccounts.slice(start, start + accountPageSize)
  }, [sortedAccounts, accountPage, accountPageSize])

  const currentPageSelectableKeys = useMemo(
    () => pagedAccounts.filter((item) => canDeleteAccount(item)).map((item) => getAccountKey(item)),
    [pagedAccounts],
  )

  const selectedCount = selectedAccounts.length
  const selectedUploadableCount = sortedAccounts.filter((item) => selectedAccounts.includes(getAccountKey(item)) && item.status === 'success' && item.task_id).length
  const currentPageSelectedCount = currentPageSelectableKeys.filter((key) => selectedAccounts.includes(key)).length
  const isCurrentPageAllSelected = currentPageSelectableKeys.length > 0 && currentPageSelectedCount === currentPageSelectableKeys.length

  useEffect(() => {
    void bootstrap()
    return () => {
      closeStream()
    }
  }, [])

  useEffect(() => {
    for (const key of expandedAccounts) {
      const element = accountLogRefs.current[key]
      if (element) {
        element.scrollTop = element.scrollHeight
      }
    }
  }, [expandedAccounts, sortedAccounts])

  useEffect(() => {
    if (accountPage > totalAccountPages) {
      setAccountPage(totalAccountPages)
    }
  }, [accountPage, totalAccountPages])

  useEffect(() => {
    const validKeys = new Set(sortedAccounts.map((item) => getAccountKey(item)))
    setSelectedAccounts((prev) => prev.filter((key) => validKeys.has(key)))
    setExpandedAccounts((prev) => prev.filter((key) => validKeys.has(key)))
  }, [sortedAccounts])

  useEffect(() => {
    const hasActiveTasks = taskSnapshots.some((item) => item.is_active && !['done', 'failed', 'stopped'].includes(item.status))
    if (!hasActiveTasks || eventSourceRef.current) return

    const timer = window.setInterval(() => {
      void (async () => {
        const snapshots = await loadTaskSnapshots(activeTask?.id)
        setActiveTask((current) => {
          if (current) {
            const matched = snapshots.find((item) => item.id === current.id)
            if (matched) return matched
          }
          return snapshots.find((item) => item.is_active && !['done', 'failed', 'stopped'].includes(item.status)) || snapshots[0] || null
        })
      })()
    }, 5000)

    return () => window.clearInterval(timer)
  }, [taskSnapshots, activeTask?.id, eventSourceRef.current])

  async function bootstrap() {
    try {
      const config = await apiFetch<Partial<FormState>>('/api/config')
      setForm((prev) => ({ ...prev, ...config }))
      await loadOutlookSummary()
      await loadLuckmailTokenSummary()
      await loadProxySummary()
      const snapshots = await loadTaskSnapshots()
      const runningSnapshot = snapshots.find((item) => item.is_active && !['done', 'failed', 'stopped'].includes(item.status))
      const nextActiveTask = runningSnapshot || snapshots[0] || null
      setActiveTask(nextActiveTask)
      if (nextActiveTask?.is_active && !['done', 'failed', 'stopped'].includes(nextActiveTask.status)) {
        await refreshTask(nextActiveTask.id)
        openStream(nextActiveTask.id)
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : '加载初始配置失败')
    } finally {
      setLoading(false)
    }
  }

  async function loadOutlookSummary() {
    const summary = await apiFetch<OutlookPoolSummary>('/api/outlook/summary')
    setOutlookSummary(summary)
  }

  async function loadLuckmailTokenSummary() {
    const summary = await apiFetch<LuckMailTokenPoolSummary>('/api/luckmail-pool/summary')
    setLuckmailTokenSummary(summary)
  }

  async function loadProxySummary() {
    const summary = await apiFetch<ProxyPoolSummary>('/api/proxies/summary')
    setProxySummary(summary)
  }

  async function loadTaskSnapshots(preferTaskId?: string) {
    const history = await apiFetch<HistoryListResponse>('/api/history/tasks?page=1&page_size=30')
    const historyMap = new Map(history.items.map((item) => [item.id, item]))
    const taskIds = history.items.map((item) => item.id)
    const orderedIds = preferTaskId && !taskIds.includes(preferTaskId) ? [preferTaskId, ...taskIds] : taskIds
    const snapshots = (
      await Promise.all(
        orderedIds.map(async (taskId) => {
          try {
            const snapshot = await apiFetch<TaskSnapshot>(
              `/api/register/tasks/${taskId}${preferTaskId && taskId === preferTaskId ? '' : '?lite=1'}`,
            )
            const historyItem = historyMap.get(taskId)
            const request = historyItem?.request || {}
            return {
              ...snapshot,
              source: String((request as Record<string, unknown>).source || snapshot.source || ''),
              request,
              meta:
                ((request as Record<string, unknown>).meta as Record<string, unknown> | undefined) ||
                snapshot.meta ||
                {},
            }
          } catch {
            return null
          }
        }),
      )
    ).filter(Boolean) as TaskSnapshot[]
    setTaskSnapshots(snapshots)
    return snapshots
  }

  async function refreshTask(taskId: string) {
    try {
      const snapshot = await apiFetch<TaskSnapshot>(`/api/register/tasks/${taskId}`)
      setActiveTask(snapshot)
      setTaskSnapshots((prev) => {
        const existingIndex = prev.findIndex((item) => item.id === taskId)
        if (existingIndex >= 0) {
          const next = [...prev]
          next[existingIndex] = snapshot
          return next
        }
        return [snapshot, ...prev]
      })
      const taskMailProvider = String(snapshot.request?.mail_provider || snapshot.meta?.mail_provider || '')
      if (taskMailProvider === 'outlook_local') {
        await loadOutlookSummary()
      }
      if (['done', 'failed', 'stopped'].includes(snapshot.status)) {
        closeStream()
        await loadLuckmailTokenSummary()
      }
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err || '')
      if (message.includes('任务不存在') || message.includes('404')) {
        closeStream()
        const snapshots = await loadTaskSnapshots()
        const runningSnapshot = snapshots.find((entry) => entry.is_active && !['done', 'failed', 'stopped'].includes(entry.status))
        setActiveTask(runningSnapshot || snapshots[0] || null)
        return
      }
      throw err
    }
  }

  function closeStream() {
    eventSourceRef.current?.close()
    eventSourceRef.current = null
    if (refreshTimerRef.current !== null) {
      window.clearTimeout(refreshTimerRef.current)
      refreshTimerRef.current = null
    }
  }

  function scheduleRefresh(taskId: string, delay = 250) {
    if (!taskId) return
    if (refreshInFlightRef.current) return
    if (refreshTimerRef.current !== null) {
      window.clearTimeout(refreshTimerRef.current)
    }
    refreshTimerRef.current = window.setTimeout(() => {
      refreshTimerRef.current = null
      if (refreshInFlightRef.current) return
      refreshInFlightRef.current = true
      void (async () => {
        try {
          await refreshTask(taskId)
        } finally {
          refreshInFlightRef.current = false
        }
      })()
    }, delay)
  }

  function openStream(taskId: string) {
    closeStream()
    const source = new EventSource(`/api/register/tasks/${taskId}/events?since=0`)
    eventSourceRef.current = source
    source.onmessage = (event) => {
      try {
        const payload = JSON.parse(event.data)
        if (payload?.done || payload?.message) {
          scheduleRefresh(taskId)
        }
      } catch {
        scheduleRefresh(taskId)
      }
    }
    source.onerror = () => {
      if (eventSourceRef.current !== source) return
      scheduleRefresh(taskId, 500)
    }
  }

  function updateField<K extends keyof FormState>(key: K, value: FormState[K]) {
    setForm((prev) => ({ ...prev, [key]: value }))
  }

  function getAccountKey(item: AccountItem) {
    return `${item.task_id || 'task'}:${item.attempt_index}`
  }

  function toggleAccount(key: string) {
    setExpandedAccounts((prev) =>
      prev.includes(key) ? prev.filter((item) => item !== key) : [...prev, key],
    )
  }

  function toggleSelectAccount(key: string) {
    setSelectedAccounts((prev) => (prev.includes(key) ? prev.filter((item) => item !== key) : [...prev, key]))
  }

  function canDeleteAccount(item: AccountItem) {
    return !['pending', 'registering', 'running'].includes(item.status)
  }

  function openSettings(tab: SettingsTab = 'base') {
    setSettingsTab(tab)
    setShowSettings(true)
  }

  function selectCurrentPageAccounts() {
    if (currentPageSelectableKeys.length === 0) return
    setSelectedAccounts((prev) => Array.from(new Set([...prev, ...currentPageSelectableKeys])))
  }

  function clearSelectedAccounts() {
    setSelectedAccounts([])
  }

  function requestDeleteAccounts(items: AccountItem[]) {
    const targets = items.filter((item) => item.task_id && canDeleteAccount(item))
    if (targets.length === 0) return
    setDeleteDialog({ items: targets })
  }

  function closeDeleteDialog() {
    if (deletingAccounts) return
    setDeleteDialog(null)
  }

  async function saveDefaults() {
    setSaving(true)
    setError('')
    try {
      await apiFetch('/api/config', {
        method: 'PUT',
        body: JSON.stringify({
          values: {
            executor_type: form.executor_type,
            mail_provider: form.mail_provider,
            use_proxy: form.use_proxy,
            luckmail_base_url: form.luckmail_base_url,
            luckmail_api_key: form.luckmail_api_key,
            luckmail_email_type: form.luckmail_email_type,
            luckmail_domain: form.luckmail_domain,
            tempmail_api_base: form.tempmail_api_base,
            cpa_api_url: form.cpa_api_url,
            cpa_api_key: form.cpa_api_key,
            sub2api_api_url: form.sub2api_api_url,
            sub2api_api_key: form.sub2api_api_key,
            sub2api_group_ids: form.sub2api_group_ids,
            codexproxy_api_url: form.codexproxy_api_url,
            codexproxy_admin_key: form.codexproxy_admin_key,
            codexproxy_proxy_url: form.codexproxy_proxy_url,
            auto_upload_target: form.auto_upload_target,
            inbound_upload_auth_token: form.inbound_upload_auth_token,
            inbound_upload_debug_logging: form.inbound_upload_debug_logging,
          },
        }),
      })
    } catch (err) {
      setError(err instanceof Error ? err.message : '保存默认配置失败')
    } finally {
      setSaving(false)
    }
  }

  async function importOutlookPool() {
    if (!outlookImportText.trim()) {
      setError('请先粘贴微软邮箱令牌内容')
      return
    }
    setImportingOutlook(true)
    setError('')
    try {
      const result = await apiFetch<OutlookImportResult>('/api/outlook/batch-import', {
        method: 'POST',
        body: JSON.stringify({ data: outlookImportText, enabled: true }),
      })
      setOutlookImportResult(result)
      setOutlookSummary(result.summary)
      setOutlookImportText('')
    } catch (err) {
      setError(err instanceof Error ? err.message : '导入微软邮箱池失败')
    } finally {
      setImportingOutlook(false)
    }
  }

  async function importLuckmailTokenPool() {
    if (!luckmailTokenImportText.trim()) {
      setError('请先粘贴邮箱令牌内容')
      return
    }
    setImportingLuckmailToken(true)
    setError('')
    try {
      const result = await apiFetch<LuckMailTokenImportResult>('/api/luckmail-pool/batch-import', {
        method: 'POST',
        body: JSON.stringify({ data: luckmailTokenImportText, enabled: true }),
      })
      setLuckmailTokenImportResult(result)
      setLuckmailTokenSummary(result.summary)
      setLuckmailTokenImportText('')
    } catch (err) {
      setError(err instanceof Error ? err.message : '导入令牌池失败')
    } finally {
      setImportingLuckmailToken(false)
    }
  }

  async function deleteOutlookAccount(accountId: number) {
    setDeletingOutlookId(accountId)
    setError('')
    try {
      const result = await apiFetch<{ ok: boolean; summary: OutlookPoolSummary }>(`/api/outlook/accounts/${accountId}`, {
        method: 'DELETE',
      })
      setOutlookSummary(result.summary)
    } catch (err) {
      setError(err instanceof Error ? err.message : '删除邮箱池账号失败')
    } finally {
      setDeletingOutlookId(null)
    }
  }

  async function deleteOutlookAccounts(scope: OutlookDeleteScope) {
    setDeletingOutlookScope(scope)
    setError('')
    try {
      const result = await apiFetch<{ ok: boolean; deleted: number; summary: OutlookPoolSummary }>(
        `/api/outlook/accounts?scope=${scope}`,
        { method: 'DELETE' },
      )
      setOutlookSummary(result.summary)
      if (scope === 'all') {
        setOutlookImportResult(null)
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : '批量删除邮箱池失败')
    } finally {
      setDeletingOutlookScope(null)
    }
  }

  async function deleteLuckmailTokenAccount(accountId: number) {
    setDeletingLuckmailTokenId(accountId)
    setError('')
    try {
      const result = await apiFetch<{ ok: boolean; summary: LuckMailTokenPoolSummary }>(`/api/luckmail-pool/accounts/${accountId}`, {
        method: 'DELETE',
      })
      setLuckmailTokenSummary(result.summary)
    } catch (err) {
      setError(err instanceof Error ? err.message : '删除令牌池账号失败')
    } finally {
      setDeletingLuckmailTokenId(null)
    }
  }

  async function deleteLuckmailTokenAccounts(scope: OutlookDeleteScope) {
    setDeletingLuckmailTokenScope(scope)
    setError('')
    try {
      const result = await apiFetch<{ ok: boolean; deleted: number; summary: LuckMailTokenPoolSummary }>(
        `/api/luckmail-pool/accounts?scope=${scope}`,
        { method: 'DELETE' },
      )
      setLuckmailTokenSummary(result.summary)
      if (scope === 'all') {
        setLuckmailTokenImportResult(null)
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : '批量删除令牌池失败')
    } finally {
      setDeletingLuckmailTokenScope(null)
    }
  }

  async function importProxyPool() {
    if (!proxyImportText.trim()) {
      setError('请先粘贴代理内容')
      return
    }
    setImportingProxy(true)
    setError('')
    try {
      const result = await apiFetch<ProxyImportResult>('/api/proxies/batch-import', {
        method: 'POST',
        body: JSON.stringify({ data: proxyImportText, enabled: true }),
      })
      setProxyImportResult(result)
      setProxySummary(result.summary)
      setProxyImportText('')
    } catch (err) {
      setError(err instanceof Error ? err.message : '导入代理池失败')
    } finally {
      setImportingProxy(false)
    }
  }

  async function testProxyPool() {
    setTestingProxies(true)
    setError('')
    try {
      const result = await apiFetch<{ ok: boolean; tested: number; success: number; failed: number; summary: ProxyPoolSummary }>('/api/proxies/test', {
        method: 'POST',
      })
      setProxySummary(result.summary)
      await loadProxySummary()
    } catch (err) {
      setError(err instanceof Error ? err.message : '代理检测失败')
      await loadProxySummary()
    } finally {
      setTestingProxies(false)
    }
  }

  async function testProxyAccount(proxyId: number) {
    setTestingProxyId(proxyId)
    setError('')
    try {
      const result = await apiFetch<{ ok: boolean; ip: string; country: string; summary: ProxyPoolSummary }>(`/api/proxies/accounts/${proxyId}/test`, {
        method: 'POST',
      })
      setProxySummary(result.summary)
      await loadProxySummary()
    } catch (err) {
      setError(err instanceof Error ? err.message : '代理检测失败')
      await loadProxySummary()
    } finally {
      setTestingProxyId(null)
    }
  }

  async function deleteProxyAccount(proxyId: number) {
    setDeletingProxyId(proxyId)
    setError('')
    try {
      const result = await apiFetch<{ ok: boolean; summary: ProxyPoolSummary }>(`/api/proxies/accounts/${proxyId}`, {
        method: 'DELETE',
      })
      setProxySummary(result.summary)
    } catch (err) {
      setError(err instanceof Error ? err.message : '删除代理失败')
    } finally {
      setDeletingProxyId(null)
    }
  }

  async function deleteProxyAccounts(scope: ProxyDeleteScope) {
    setDeletingProxyScope(scope)
    setError('')
    try {
      const result = await apiFetch<{ ok: boolean; deleted: number; summary: ProxyPoolSummary }>(`/api/proxies/accounts?scope=${scope}`, {
        method: 'DELETE',
      })
      setProxySummary(result.summary)
      setProxyImportResult(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : '清空代理池失败')
    } finally {
      setDeletingProxyScope(null)
    }
  }

  async function startTask(event: FormEvent) {
    event.preventDefault()
    setStarting(true)
    setError('')
    setExpandedAccounts([])
    try {
      const response = isRunning && activeTask
        ? await apiFetch<{ task_id: string }>(`/api/register/tasks/${activeTask.id}/append`, {
            method: 'POST',
            body: JSON.stringify({ count: Number(form.count) }),
          })
        : await apiFetch<{ task_id: string }>('/api/register/tasks', {
            method: 'POST',
            body: JSON.stringify({
              count: Number(form.count),
              concurrency: Number(form.concurrency),
              register_delay_seconds: Number(form.register_delay_seconds),
              email: null,
              password: null,
              proxy: null,
              use_proxy: form.use_proxy,
              executor_type: form.executor_type,
              mail_provider: form.mail_provider,
              provider_config: {
                luckmail_base_url: form.luckmail_base_url,
                luckmail_api_key: form.luckmail_api_key,
                luckmail_email_type: form.luckmail_email_type,
                luckmail_domain: form.luckmail_domain,
                tempmail_api_base: form.tempmail_api_base,
                cpa_api_url: form.cpa_api_url,
                cpa_api_key: form.cpa_api_key,
                sub2api_api_url: form.sub2api_api_url,
                sub2api_api_key: form.sub2api_api_key,
                sub2api_group_ids: form.sub2api_group_ids,
                codexproxy_api_url: form.codexproxy_api_url,
                codexproxy_admin_key: form.codexproxy_admin_key,
                codexproxy_proxy_url: form.codexproxy_proxy_url,
                auto_upload_target: form.auto_upload_target,
              },
              phone_config: {},
            }),
          })
      await loadTaskSnapshots(response.task_id)
      await refreshTask(response.task_id)
      openStream(response.task_id)
    } catch (err) {
      setError(err instanceof Error ? err.message : (isRunning ? '追加账号失败' : '创建任务失败'))
    } finally {
      setStarting(false)
    }
  }

  async function retryAccount(item: AccountItem) {
    const retryKey = item.id || item.attempt_index
    setRetryingResultId(retryKey)
    setError('')
    setExpandedAccounts([getAccountKey(item)])
    try {
      const targetTaskId = isRunning && activeTask?.id ? activeTask.id : ''
      const response = item.id
        ? await apiFetch<{ task_id: string }>(`/api/register/results/${item.id}/retry${targetTaskId ? `?target_task_id=${encodeURIComponent(targetTaskId)}` : ''}`, {
            method: 'POST',
          })
        : await apiFetch<{ task_id: string }>(`/api/register/tasks/${item.task_id}/attempts/${item.attempt_index}/retry${targetTaskId ? `?target_task_id=${encodeURIComponent(targetTaskId)}` : ''}`, {
            method: 'POST',
          })
      await loadTaskSnapshots(response.task_id)
      await refreshTask(response.task_id)
      openStream(response.task_id)
    } catch (err) {
      setError(err instanceof Error ? err.message : '重试失败')
    } finally {
      setRetryingResultId(null)
    }
  }

  async function performDeleteAccounts(targets: AccountItem[]) {
    if (targets.length === 0) return
    setDeletingAccounts(true)
    setError('')
    const deletedKeys = new Set(targets.map((item) => getAccountKey(item)))
    try {
      if (targets.length === 1) {
        const item = targets[0]
        await apiFetch('/api/register/accounts/delete', {
          method: 'POST',
          body: JSON.stringify({
            task_id: item.task_id,
            task_ids: item.task_ids || [],
            refs: item.task_refs || [],
            attempt_index: item.attempt_index,
          }),
        })
      } else {
        await apiFetch('/api/register/accounts/delete-batch', {
          method: 'POST',
          body: JSON.stringify({
            items: targets.map((item) => ({
              task_id: item.task_id,
              task_ids: item.task_ids || [],
              refs: item.task_refs || [],
              attempt_index: item.attempt_index,
            })),
          }),
        })
      }
      setExpandedAccounts((prev) => prev.filter((key) => !deletedKeys.has(key)))
      setSelectedAccounts((prev) => prev.filter((key) => !deletedKeys.has(key)))
      setDeleteDialog(null)
      setDeletingAccounts(false)
      void (async () => {
        const snapshots = await loadTaskSnapshots(activeTask?.id)
        const runningSnapshot = snapshots.find((entry) => entry.is_active && !['done', 'failed', 'stopped'].includes(entry.status))
        setActiveTask((current) => {
          if (current) {
            const matched = snapshots.find((entry) => entry.id === current.id)
            if (matched) return matched
          }
          return runningSnapshot || snapshots[0] || null
        })
      })()
    } catch (err) {
      setError(err instanceof Error ? err.message : targets.length === 1 ? '删除账号失败' : '批量删除失败')
      setDeleteDialog(null)
      setDeletingAccounts(false)
    }
  }

  async function deleteAccount(item: AccountItem) {
    requestDeleteAccounts([item])
  }

  async function deleteSelectedAccounts() {
    const targets = sortedAccounts.filter((item) => selectedAccounts.includes(getAccountKey(item)) && canDeleteAccount(item) && item.task_id)
    if (targets.length === 0) return
    requestDeleteAccounts(targets)
  }

  async function uploadSelectedAccounts(target: UploadTarget) {
    const targets = sortedAccounts.filter((item) => selectedAccounts.includes(getAccountKey(item)) && item.status === 'success' && item.task_id)
    if (targets.length === 0) return
    setUploadingTarget(target)
    setError('')
    try {
      const response = await apiFetch<{ ok: boolean; uploaded: number; failed: number; skipped: number; items: Array<{ ok: boolean; message: string }> }>(
        '/api/register/accounts/upload',
        {
          method: 'POST',
          body: JSON.stringify({
            target,
            items: targets.map((item) => ({
              task_id: item.task_id,
              task_ids: item.task_ids || [],
              refs: item.task_refs || [],
              attempt_index: item.attempt_index,
            })),
          }),
        },
      )
      if (response.failed > 0) {
        const firstError = response.items.find((item) => !item.ok)?.message || '上传失败'
        setError(firstError)
      } else if (response.uploaded === 0) {
        setError('没有可上传的成功账号')
      } else {
        setError('')
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : '上传失败')
    } finally {
      setUploadingTarget(null)
    }
  }

  async function exportSelectedAccounts() {
    const targets = sortedAccounts.filter((item) => selectedAccounts.includes(getAccountKey(item)) && item.status === 'success' && item.task_id)
    if (targets.length === 0) return
    setExportingZip(true)
    setError('')
    try {
      const response = await fetch('/api/register/accounts/export', {
        method: 'POST',
        cache: 'no-store',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          items: targets.map((item) => ({
            task_id: item.task_id,
            task_ids: item.task_ids || [],
            refs: item.task_refs || [],
            attempt_index: item.attempt_index,
          })),
        }),
      })
      if (!response.ok) {
        const text = await response.text()
        throw new Error(text || '导出失败')
      }
      const blob = await response.blob()
      const disposition = response.headers.get('Content-Disposition') || ''
      const match = disposition.match(/filename="?([^"]+)"?/)
      const filename = match?.[1] || `accounts_export_${Date.now()}.zip`
      const objectUrl = URL.createObjectURL(blob)
      const anchor = document.createElement('a')
      anchor.href = objectUrl
      anchor.download = filename
      document.body.appendChild(anchor)
      anchor.click()
      anchor.remove()
      URL.revokeObjectURL(objectUrl)
    } catch (err) {
      setError(err instanceof Error ? err.message : '导出失败')
    } finally {
      setExportingZip(false)
    }
  }

  async function stopAccount(item: AccountItem) {
    const actionTaskId = item.action_task_id || item.task_id
    const actionAttemptIndex = Number(item.action_attempt_index || item.attempt_index || 0)
    if (!actionTaskId || actionAttemptIndex <= 0 || !canStopAccount(item)) return
    const accountKey = getAccountKey(item)
    setStoppingAccountKeys((prev) => (prev.includes(accountKey) ? prev : [...prev, accountKey]))
    setError('')
    try {
      await apiFetch(`/api/register/tasks/${actionTaskId}/attempts/${actionAttemptIndex}/stop`, { method: 'POST' })
      await refreshTask(actionTaskId)
      if (activeTask?.id && activeTask.id !== actionTaskId) {
        await refreshTask(activeTask.id)
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : '停止账号失败')
    } finally {
      setStoppingAccountKeys((prev) => prev.filter((key) => key !== accountKey))
    }
  }

  async function sendControl(action: 'stop' | 'skip-current') {
    if (!activeTask) return
    try {
      await apiFetch(`/api/register/tasks/${activeTask.id}/${action}`, { method: 'POST' })
      await refreshTask(activeTask.id)
    } catch (err) {
      setError(err instanceof Error ? err.message : '任务控制失败')
    }
  }

  function renderSettingsContent() {
    if (settingsTab === 'base') {
      return (
        <div className="settings-pane-body">
          <div className="sub-block">
            <div className="sub-block-title">Auto Upload</div>
            <label>
              <span>Default target after success</span>
              <select value={form.auto_upload_target} onChange={(e) => updateField('auto_upload_target', e.target.value as AutoUploadTarget)}>
                <option value="none">Do not upload</option>
                <option value="cpa">CPA only</option>
                <option value="sub2api">Sub2API only</option>
                <option value="codexproxy">CodexProxy only</option>
                <option value="both">CPA + Sub2API</option>
                <option value="all">Upload all</option>
              </select>
            </label>
          </div>
          <div className="sub-block">
            <div className="sub-block-title">Inbound Upload</div>
            <label>
              <span>Shared auth token</span>
              <input
                type="password"
                value={form.inbound_upload_auth_token}
                onChange={(e) => updateField('inbound_upload_auth_token', e.target.value)}
                placeholder="used by /api/inbound/outlook-upload"
              />
            </label>
            <label className="checkbox-row settings-checkbox-row">
              <input
                type="checkbox"
                checked={form.inbound_upload_debug_logging}
                onChange={(e) => updateField('inbound_upload_debug_logging', e.target.checked)}
              />
              <span>Enable debug logging for inbound uploads</span>
            </label>
          </div>
          <div className="sub-block">
            <div className="sub-block-title">默认执行设置</div>
            <div className="field-group two-col compact">
              <label>
                <span>默认执行器</span>
                <select value={form.executor_type} onChange={(e) => updateField('executor_type', e.target.value as Executor)}>
                  <option value="protocol">protocol</option>
                  <option value="headless">headless</option>
                  <option value="headed">headed</option>
                </select>
              </label>
              <label>
                <span>默认邮箱 provider</span>
                <select value={form.mail_provider} onChange={(e) => updateField('mail_provider', e.target.value as MailProvider)}>
                  <option value="luckmail">LuckMail</option>
                  <option value="tempmail_lol">TempMail.lol</option>
                  <option value="outlook_local">Outlook（本地微软令牌）</option>
                </select>
              </label>
            </div>

            <label className="checkbox-row settings-checkbox-row">
              <input type="checkbox" checked={form.use_proxy} onChange={(e) => updateField('use_proxy', e.target.checked)} />
              <span>默认使用代理</span>
            </label>
          </div>

          <div className="sub-block">
            <div className="sub-block-title">代理池</div>
            <div className="mini-stats-grid">
              <StatCard label="总数" value={proxySummary?.total ?? 0} />
              <StatCard label="可用" value={proxySummary?.enabled ?? 0} tone="success" />
              <StatCard label="连通" value={proxySummary?.healthy ?? 0} tone="info" />
              <StatCard label="失败" value={proxySummary?.unhealthy ?? 0} tone="danger" />
              <StatCard label="成功次数" value={proxySummary?.success_count ?? 0} tone="success" />
              <StatCard label="失败次数" value={proxySummary?.failure_count ?? 0} tone="danger" />
            </div>

            <label>
              <span>批量导入内容</span>
              <textarea
                rows={6}
                value={proxyImportText}
                onChange={(e) => setProxyImportText(e.target.value)}
                placeholder={'host:port\nuser:pass@host:port'}
              />
            </label>

            <div className="form-actions split-actions">
              <button className="primary-btn" type="button" onClick={() => void importProxyPool()} disabled={importingProxy}>
                {importingProxy ? '导入中...' : '批量导入'}
              </button>
              <button className="ghost-btn" type="button" onClick={() => void testProxyPool()} disabled={testingProxies}>
                {testingProxies ? '检测中...' : '检测全部'}
              </button>
              <button
                className="ghost-btn danger"
                type="button"
                onClick={() => void deleteProxyAccounts('all')}
                disabled={deletingProxyScope !== null}
              >
                {deletingProxyScope === 'all' ? '删除中...' : '删除全部'}
              </button>
            </div>

            {proxyImportResult ? (
              <div className="import-result">
                <div className="import-result-head">
                  <strong>最近一次导入</strong>
                  <span>成功 {proxyImportResult.success} / 更新 {proxyImportResult.updated} / 失败 {proxyImportResult.failed}</span>
                </div>
                {proxyImportResult.errors.length > 0 ? (
                  <div className="import-error-list">
                    {proxyImportResult.errors.slice(0, 4).map((item, index) => (
                      <div className="import-error-item" key={`${item}-${index}`}>{item}</div>
                    ))}
                  </div>
                ) : null}
              </div>
            ) : null}

            <div className="provider-item-list">
              {(proxySummary?.items || []).length === 0 ? <div className="timeline-empty">暂无数据</div> : null}
              {(proxySummary?.items || []).map((item) => (
                <div className="provider-item provider-item-compact" key={item.id}>
                  <div className="provider-item-main">
                    <strong className="proxy-line">{item.proxy_url}</strong>
                    <p>
                      {item.last_check_status === 'ok'
                        ? `${item.last_ip || '-'}${item.last_country ? ` (${item.last_country})` : ''}`
                        : item.last_check_message || '-'}
                    </p>
                  </div>
                  <div className="provider-item-meta">
                    <span className="proxy-count proxy-count-success">{item.success_count}</span>
                    <span className="proxy-count proxy-count-failed">{item.failure_count}</span>
                    <button
                      className="tiny-action-btn"
                      type="button"
                      onClick={() => void testProxyAccount(item.id)}
                      disabled={testingProxyId === item.id}
                    >
                      {testingProxyId === item.id ? '检测中' : '测试'}
                    </button>
                    <button
                      className="tiny-action-btn danger"
                      type="button"
                      onClick={() => void deleteProxyAccount(item.id)}
                      disabled={deletingProxyId === item.id || testingProxyId === item.id}
                    >
                      {deletingProxyId === item.id ? '删除中' : '删除'}
                    </button>
                  </div>
                </div>
              ))}
            </div>
          </div>

        </div>
      )
    }

    if (settingsTab === 'mail') {
      return (
        <div className="settings-pane-body">
          <div className="sub-block">
            <div className="sub-block-title">LuckMail</div>
            <label>
              <span>Base URL</span>
              <input value={form.luckmail_base_url} onChange={(e) => updateField('luckmail_base_url', e.target.value)} />
            </label>
            <label>
              <span>API Key</span>
              <input value={form.luckmail_api_key} onChange={(e) => updateField('luckmail_api_key', e.target.value)} placeholder="ak_..." />
            </label>
            <div className="field-group two-col compact">
              <label>
                <span>邮箱类型</span>
                <input value={form.luckmail_email_type} onChange={(e) => updateField('luckmail_email_type', e.target.value)} placeholder="ms_graph / ms_imap" />
              </label>
              <label>
                <span>指定域名</span>
                <input value={form.luckmail_domain} onChange={(e) => updateField('luckmail_domain', e.target.value)} placeholder="outlook.com" />
              </label>
            </div>
          </div>

          <div className="sub-block">
            <div className="sub-block-title">TempMail.lol</div>
            <label>
              <span>API Base</span>
              <input value={form.tempmail_api_base} onChange={(e) => updateField('tempmail_api_base', e.target.value)} />
            </label>
          </div>
        </div>
      )
    }

    if (settingsTab === 'uploads') {
      return (
        <div className="settings-pane-body">
          <div className="sub-block">
            <div className="sub-block-title">CPA 上传</div>
            <label>
              <span>API URL</span>
              <input value={form.cpa_api_url} onChange={(e) => updateField('cpa_api_url', e.target.value)} placeholder="https://your-cpa.example.com" />
            </label>
            <label>
              <span>API Key</span>
              <input value={form.cpa_api_key} onChange={(e) => updateField('cpa_api_key', e.target.value)} placeholder="Bearer token / 可留空" />
            </label>
          </div>

          <div className="sub-block">
            <div className="sub-block-title">Sub2API 上传</div>
            <label>
              <span>API URL</span>
              <input value={form.sub2api_api_url} onChange={(e) => updateField('sub2api_api_url', e.target.value)} placeholder="https://your-sub2api.example.com" />
            </label>
            <label>
              <span>API Key</span>
              <input value={form.sub2api_api_key} onChange={(e) => updateField('sub2api_api_key', e.target.value)} placeholder="x-api-key" />
            </label>
            <label>
              <span>分组 ID</span>
              <input value={form.sub2api_group_ids} onChange={(e) => updateField('sub2api_group_ids', e.target.value)} placeholder="多个分组用英文逗号分隔，如 2,4,8" />
            </label>
          </div>

          <div className="sub-block">
            <div className="sub-block-title">CodexProxy 上传</div>
            <label>
              <span>API URL</span>
              <input value={form.codexproxy_api_url} onChange={(e) => updateField('codexproxy_api_url', e.target.value)} placeholder="http://127.0.0.1:8090" />
            </label>
            <label>
              <span>Admin Key</span>
              <input value={form.codexproxy_admin_key} onChange={(e) => updateField('codexproxy_admin_key', e.target.value)} placeholder="your-admin-secret" />
            </label>
            <label>
              <span>Proxy URL</span>
              <input value={form.codexproxy_proxy_url} onChange={(e) => updateField('codexproxy_proxy_url', e.target.value)} placeholder="optional proxy url or leave empty" />
            </label>
          </div>
        </div>
      )
    }

    return (
      <div className="settings-pane-body">
        <div className="sub-block">
          <div className="sub-block-title">Outlook 本地邮箱池</div>
          <div className="mini-stats-grid">
            <StatCard label="总数" value={outlookSummary?.total ?? 0} />
            <StatCard label="可用" value={outlookSummary?.enabled ?? 0} tone="success" />
            <StatCard label="已取出" value={outlookSummary?.disabled ?? 0} tone="danger" />
            <StatCard label="带令牌" value={outlookSummary?.with_oauth ?? 0} tone="info" />
          </div>

          <label>
            <span>批量导入内容</span>
            <textarea
              rows={6}
              value={outlookImportText}
              onChange={(e) => setOutlookImportText(e.target.value)}
              placeholder={'示例：\nuser1@outlook.com----mail_password----client_id----refresh_token\nuser2@outlook.com----mail_password'}
            />
          </label>

          <div className="form-actions split-actions">
            <button className="primary-btn" type="button" onClick={() => void importOutlookPool()} disabled={importingOutlook}>
              {importingOutlook ? '导入中...' : '批量导入邮箱池'}
            </button>
            <button className="ghost-btn" type="button" onClick={() => setOutlookImportText('')} disabled={importingOutlook}>
              清空
            </button>
          </div>

          <div className="form-actions split-actions">
            <button
              className="ghost-btn danger"
              type="button"
              onClick={() => void deleteOutlookAccounts('taken')}
              disabled={deletingOutlookScope !== null}
            >
              {deletingOutlookScope === 'taken' ? '删除中...' : '删除已取出'}
            </button>
            <button
              className="ghost-btn danger"
              type="button"
              onClick={() => void deleteOutlookAccounts('all')}
              disabled={deletingOutlookScope !== null}
            >
              {deletingOutlookScope === 'all' ? '删除中...' : '删除全部'}
            </button>
          </div>
        </div>

        {outlookImportResult ? (
          <div className="import-result">
            <div className="import-result-head">
              <strong>最近一次导入</strong>
              <span>成功 {outlookImportResult.success} / 更新 {outlookImportResult.updated} / 失败 {outlookImportResult.failed}</span>
            </div>
            {outlookImportResult.errors.length > 0 ? (
              <div className="import-error-list">
                {outlookImportResult.errors.slice(0, 4).map((item, index) => (
                  <div className="import-error-item" key={`${item}-${index}`}>{item}</div>
                ))}
              </div>
            ) : null}
          </div>
        ) : null}

        <div className="provider-item-list">
          {(outlookSummary?.items || []).length === 0 ? <div className="timeline-empty">暂无数据</div> : null}
          {(outlookSummary?.items || []).map((item) => (
            <div className="provider-item" key={item.id}>
              <div>
                <strong>{renderPoolEmail(item.email)}</strong>
                <p>{item.has_oauth ? 'OAuth / refresh token' : '密码模式'}</p>
              </div>
              <div className="provider-item-meta">
                <span className={`history-badge ${item.enabled ? 'status-done' : 'status-stopped'}`}>
                  {item.enabled ? '可用' : '已取出'}
                </span>
                <button
                  className="tiny-action-btn danger"
                  type="button"
                  onClick={() => void deleteOutlookAccount(item.id)}
                  disabled={deletingOutlookId === item.id}
                >
                  {deletingOutlookId === item.id ? '删除中' : '删除'}
                </button>
              </div>
            </div>
          ))}
        </div>

        <div className="sub-block">
          <div className="sub-block-title">LuckMail 令牌池</div>
          <div className="mini-stats-grid">
            <StatCard label="总数" value={luckmailTokenSummary?.total ?? 0} />
            <StatCard label="可用" value={luckmailTokenSummary?.enabled ?? 0} tone="success" />
            <StatCard label="已取出" value={luckmailTokenSummary?.disabled ?? 0} tone="danger" />
          </div>

          <label>
            <span>批量导入内容</span>
            <textarea
              rows={6}
              value={luckmailTokenImportText}
              onChange={(e) => setLuckmailTokenImportText(e.target.value)}
              placeholder={'示例：\nuser1@hotmail.com----tok_xxx\nuser2@hotmail.com----tok_xxx'}
            />
          </label>

          <div className="form-actions split-actions">
            <button className="primary-btn" type="button" onClick={() => void importLuckmailTokenPool()} disabled={importingLuckmailToken}>
              {importingLuckmailToken ? '导入中...' : '批量导入令牌池'}
            </button>
            <button className="ghost-btn" type="button" onClick={() => setLuckmailTokenImportText('')} disabled={importingLuckmailToken}>
              清空
            </button>
          </div>

          <div className="form-actions split-actions">
            <button
              className="ghost-btn danger"
              type="button"
              onClick={() => void deleteLuckmailTokenAccounts('taken')}
              disabled={deletingLuckmailTokenScope !== null}
            >
              {deletingLuckmailTokenScope === 'taken' ? '删除中...' : '删除已取出'}
            </button>
            <button
              className="ghost-btn danger"
              type="button"
              onClick={() => void deleteLuckmailTokenAccounts('all')}
              disabled={deletingLuckmailTokenScope !== null}
            >
              {deletingLuckmailTokenScope === 'all' ? '删除中...' : '删除全部'}
            </button>
          </div>
        </div>

        {luckmailTokenImportResult ? (
          <div className="import-result">
            <div className="import-result-head">
              <strong>最近一次导入</strong>
              <span>成功 {luckmailTokenImportResult.success} / 更新 {luckmailTokenImportResult.updated} / 失败 {luckmailTokenImportResult.failed}</span>
            </div>
            {luckmailTokenImportResult.errors.length > 0 ? (
              <div className="import-error-list">
                {luckmailTokenImportResult.errors.slice(0, 4).map((item, index) => (
                  <div className="import-error-item" key={`${item}-${index}`}>{item}</div>
                ))}
              </div>
            ) : null}
          </div>
        ) : null}

        <div className="provider-item-list">
          {(luckmailTokenSummary?.items || []).length === 0 ? <div className="timeline-empty">暂无数据</div> : null}
          {(luckmailTokenSummary?.items || []).map((item) => (
            <div className="provider-item" key={item.id}>
              <div>
                <strong>{renderPoolEmail(item.email)}</strong>
                <p>{item.token ? `${String(item.token).slice(0, 14)}...` : '-'}</p>
              </div>
              <div className="provider-item-meta">
                <span className={`history-badge ${item.enabled ? 'status-done' : 'status-stopped'}`}>
                  {item.enabled ? '可用' : '已取出'}
                </span>
                <button
                  className="tiny-action-btn danger"
                  type="button"
                  onClick={() => void deleteLuckmailTokenAccount(item.id)}
                  disabled={deletingLuckmailTokenId === item.id}
                >
                  {deletingLuckmailTokenId === item.id ? '删除中' : '删除'}
                </button>
              </div>
            </div>
          ))}
        </div>
      </div>
    )
  }

  const activeSettingsMeta = settingsTabs.find((item) => item.key === settingsTab) || settingsTabs[0]

  return (
    <div className="page-shell">
      <div className="page-backdrop" />
      {error ? <div className="error-banner">{error}</div> : null}

      <main className="workbench-grid two-pane">
        <section className="panel form-panel">
          <div className="panel-title-row">
            <h2>注册配置器</h2>
            <button className="secondary-btn small-btn" type="button" onClick={() => openSettings('base')}>
              设置
            </button>
          </div>
          <form className="config-form" onSubmit={startTask}>
            <div className="field-group two-col">
              <label>
                <span>批量数量</span>
                <input type="number" min={1} max={9999} value={form.count} onChange={(e) => updateField('count', Number(e.target.value))} />
              </label>
              <label>
                <span>并发数</span>
                <input type="number" min={1} max={100} value={form.concurrency} onChange={(e) => updateField('concurrency', Number(e.target.value))} />
              </label>
            </div>

            <div className="field-group two-col">
              <label>
                <span>启动延迟（秒）</span>
                <input type="number" min={0} step={0.5} value={form.register_delay_seconds} onChange={(e) => updateField('register_delay_seconds', Number(e.target.value))} />
              </label>
              <label>
                <span>执行器</span>
                <select value={form.executor_type} onChange={(e) => updateField('executor_type', e.target.value as Executor)}>
                  <option value="protocol">protocol</option>
                  <option value="headless">headless</option>
                  <option value="headed">headed</option>
                </select>
              </label>
            </div>

            <label className="checkbox-row">
              <input type="checkbox" checked={form.use_proxy} onChange={(e) => updateField('use_proxy', e.target.checked)} />
              <span>使用代理</span>
            </label>

            <label>
              <span>邮箱 provider</span>
              <select value={form.mail_provider} onChange={(e) => updateField('mail_provider', e.target.value as MailProvider)}>
                <option value="luckmail">LuckMail</option>
                <option value="tempmail_lol">TempMail.lol</option>
                <option value="outlook_local">Outlook（本地微软令牌）</option>
              </select>
            </label>

            {form.mail_provider === 'outlook_local' ? (
              <div className="inline-meta-row">
                <span>剩余邮箱：{outlookSummary?.enabled ?? 0}</span>
                <span>已取出：{outlookSummary?.disabled ?? 0}</span>
              </div>
            ) : null}

            <label>
              <span>Auto upload after success</span>
              <select value={form.auto_upload_target} onChange={(e) => updateField('auto_upload_target', e.target.value as AutoUploadTarget)}>
                <option value="none">Do not upload</option>
                <option value="cpa">CPA only</option>
                <option value="sub2api">Sub2API only</option>
                <option value="codexproxy">CodexProxy only</option>
                <option value="both">CPA + Sub2API</option>
                <option value="all">Upload all</option>
              </select>
            </label>

            <div className="form-actions">
              <button className="primary-btn" type="submit" disabled={starting || loading}>
                {starting ? (isRunning ? '追加中...' : '任务创建中...') : isRunning ? '追加账号' : '开始注册'}
              </button>
            </div>
          </form>
        </section>

        <section className="panel account-panel">
          <div className="panel-title-row">
            <h2>账号列表</h2>
            <span>{activeTask?.id || '-'}{activeTask?.source === 'external_upload' ? ' · 外部触发' : ''}</span>
          </div>

          <div className="stats-grid compact-stats">
            <StatCard label="状态" value={getStatusLabel(activeTask?.status || 'idle')} tone={isRunning ? 'info' : 'default'} />
            <StatCard label="进度" value={`${taskStats.completed}/${taskStats.total}`} />
            <StatCard label="成功" value={taskStats.success} tone="success" />
            <StatCard label="失败" value={taskStats.failed} tone="danger" />
          </div>

          <div className="control-row">
            <button className="ghost-btn danger" type="button" onClick={() => void sendControl('stop')} disabled={!isRunning}>停止任务</button>
            <button className="ghost-btn" type="button" onClick={() => selectCurrentPageAccounts()} disabled={currentPageSelectableKeys.length === 0 || isCurrentPageAllSelected}>
              全选
            </button>
            <button className="ghost-btn" type="button" onClick={() => clearSelectedAccounts()} disabled={selectedCount === 0}>
              取消选择
            </button>
            <div className="selected-count-chip">{selectedCount}</div>
            <button className="ghost-btn" type="button" onClick={() => void uploadSelectedAccounts('cpa')} disabled={uploadingTarget !== null || selectedUploadableCount === 0}>
              {uploadingTarget === 'cpa' ? '上传中...' : '上传CPA'}
            </button>
            <button className="ghost-btn" type="button" onClick={() => void uploadSelectedAccounts('sub2api')} disabled={uploadingTarget !== null || selectedUploadableCount === 0}>
              {uploadingTarget === 'sub2api' ? '上传中...' : '上传Sub2API'}
            </button>
            <button className="ghost-btn" type="button" onClick={() => void uploadSelectedAccounts('codexproxy')} disabled={uploadingTarget !== null || selectedUploadableCount === 0}>
              {uploadingTarget === 'codexproxy' ? '上传中...' : '上传CodexProxy'}
            </button>
            <button className="ghost-btn" type="button" onClick={() => void exportSelectedAccounts()} disabled={exportingZip || selectedUploadableCount === 0}>
              {exportingZip ? '导出中...' : '导出ZIP'}
            </button>
            <button className="ghost-btn" type="button" onClick={() => void deleteSelectedAccounts()} disabled={deletingAccounts || selectedCount === 0}>
              {deletingAccounts ? '删除中...' : '删除选中'}
            </button>
          </div>

          <div className="account-list">
            {sortedAccounts.length === 0 ? <div className="timeline-empty">暂无账号</div> : null}
            {pagedAccounts.map((item) => {
              const accountKey = getAccountKey(item)
              const expanded = expandedAccounts.includes(accountKey)
              const selected = selectedAccounts.includes(accountKey)
              const deletable = canDeleteAccount(item)
              const stopping = stoppingAccountKeys.includes(accountKey)
              const showAccountError = isTerminalAccountStatus(item.status) && Boolean(item.error || item.failure_detail)
              return (
                <div className={`account-card status-${item.status}`} key={accountKey}>
                  <div className="account-card-head">
                    <div className="account-main-wrap">
                      <label className={`account-check ${!deletable ? 'disabled' : ''}`}>
                        <input
                          type="checkbox"
                          checked={selected}
                          disabled={!deletable || deletingAccounts}
                          onChange={() => toggleSelectAccount(accountKey)}
                        />
                      </label>
                      <button className="account-main" type="button" onClick={() => toggleAccount(accountKey)}>
                        <span className="account-title-row">
                          <strong>{getAccountTitle(item)}</strong>
                          <span className={`account-inline-status ${getInlineStatusClass(item.status)}`}>
                            {isActiveAccountStatus(item.status) ? getActiveFlowLabel(item) : getStatusLabel(item.status)}
                          </span>
                        </span>
                      </button>
                    </div>
                    <div className="account-side">
                      {canStopAccount(item) ? (
                        <button
                          className="tiny-action-btn warning"
                          type="button"
                          onClick={() => void stopAccount(item)}
                          disabled={stopping}
                        >
                          {stopping ? '停止中' : '停止'}
                        </button>
                      ) : null}
                      <button
                        className="tiny-action-btn danger"
                        type="button"
                        onClick={() => void deleteAccount(item)}
                        disabled={!deletable || deletingAccounts || stopping}
                      >
                        删除
                      </button>
                      <button className="text-btn account-expand-btn" type="button" onClick={() => toggleAccount(accountKey)}>
                        {expanded ? '收起' : '展开'}
                      </button>
                    </div>
                  </div>

                  {['failed', 'stopped'].includes(item.status) ? (
                    <div className="account-stage failed">
                      <span>失败阶段：{item.failure_stage_label || item.failure_stage || '-'}</span>
                      {item.retry_supported ? (
                        <button
                          className="tiny-action-btn"
                          type="button"
                          onClick={() => void retryAccount(item)}
                          disabled={retryingResultId === (item.id || item.attempt_index)}
                        >
                          {retryingResultId === (item.id || item.attempt_index) ? '重试中' : '从这里重试'}
                        </button>
                      ) : null}
                    </div>
                  ) : null}

                  {showAccountError && item.error ? <div className="account-error">{item.error}</div> : null}
                  {showAccountError && !item.error && item.failure_detail ? <div className="account-error">{item.failure_detail}</div> : null}

                  {expanded ? (
                    <div className="account-log-list">
                      {item.logs.length === 0 ? (
                        <div className="timeline-empty">暂无日志</div>
                      ) : (
                        <div
                          className="account-log-box"
                          ref={(node) => {
                            accountLogRefs.current[accountKey] = node
                          }}
                        >
                          {item.logs.map((line, index) => (
                            <div className={`account-log-line tone-${getLogTone(line)}`} key={`${accountKey}-${index}`}>
                              {formatAccountLog(line)}
                            </div>
                          ))}
                        </div>
                      )}
                    </div>
                  ) : null}
                </div>
              )
            })}
          </div>

          <div className="pagination-row">
            <button className="ghost-btn page-btn" type="button" onClick={() => setAccountPage((prev) => Math.max(1, prev - 1))} disabled={accountPage <= 1}>
              上一页
            </button>
            <div className="page-indicator">{accountPage} / {totalAccountPages}</div>
            <button className="ghost-btn page-btn" type="button" onClick={() => setAccountPage((prev) => Math.min(totalAccountPages, prev + 1))} disabled={accountPage >= totalAccountPages}>
              下一页
            </button>
            <select
              className="page-size-select"
              value={accountPageSize}
              onChange={(e) => {
                setAccountPageSize(Number(e.target.value))
                setAccountPage(1)
              }}
            >
              {accountPageSizeOptions.map((size) => (
                <option key={size} value={size}>{size}</option>
              ))}
            </select>
          </div>
        </section>
      </main>

      {showSettings ? (
        <div className="modal-shell" onClick={() => setShowSettings(false)}>
          <div className="modal-card settings-modal" onClick={(e) => e.stopPropagation()}>
            <div className="panel-title-row settings-header">
              <div>
                <h2>设置</h2>
              </div>
              <div className="hero-actions">
                <button className="secondary-btn small-btn" type="button" onClick={() => void saveDefaults()} disabled={saving}>
                  {saving ? '保存中...' : '保存设置'}
                </button>
                <button className="text-btn" type="button" onClick={() => setShowSettings(false)}>关闭</button>
              </div>
            </div>

            <div className="settings-layout">
              <aside className="settings-sidebar">
                {settingsTabs.map((item) => (
                  <button
                    key={item.key}
                    type="button"
                    className={`settings-nav-btn ${settingsTab === item.key ? 'active' : ''}`}
                    onClick={() => setSettingsTab(item.key)}
                  >
                    <strong>{item.label}</strong>
                  </button>
                ))}
              </aside>

              <section className="settings-content">
                <div className="settings-content-head">
                  <strong>{activeSettingsMeta.label}</strong>
                </div>
                {renderSettingsContent()}
              </section>
            </div>
          </div>
        </div>
      ) : null}

      {deleteDialog ? (
        <div className="modal-shell" onClick={() => closeDeleteDialog()}>
          <div className="modal-card confirm-modal" onClick={(e) => e.stopPropagation()}>
            <div className="confirm-title">删除 {deleteDialog.items.length} 个账号？</div>
            <div className="confirm-actions">
              <button className="ghost-btn" type="button" onClick={() => closeDeleteDialog()} disabled={deletingAccounts}>
                取消
              </button>
              <button className="ghost-btn danger" type="button" onClick={() => void performDeleteAccounts(deleteDialog.items)} disabled={deletingAccounts}>
                {deletingAccounts ? '删除中...' : '删除'}
              </button>
            </div>
          </div>
        </div>
      ) : null}

      {loading ? <div className="loading-overlay">加载中...</div> : null}
    </div>
  )
}
