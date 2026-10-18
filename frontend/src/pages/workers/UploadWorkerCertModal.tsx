// Форма загрузки серверного серта API воркера (arch/adminpanel/03 §3.7): два
// PEM-поля (серт / приватный ключ PKCS#8) + выбор файла; клиентская валидация —
// зеркало серверной (§4.3), сервер — истина. Отказы 422 («влияет на исходящие»)
// и 400 — баннер с текстом ProblemDetails.
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Alert, Button, FileButton, Group, Modal, Stack, Textarea } from '@mantine/core';
import { useState } from 'react';
import { ApiError } from '../../api/client';
import { uploadWorkerApiCert, workerQueryKeys } from '../../api/queries';

export function UploadWorkerCertModal({ worker, opened, onClose }: {
  worker: string;
  opened: boolean;
  onClose: () => void;
}) {
  const queryClient = useQueryClient();
  const [certPem, setCertPem] = useState('');
  const [keyPem, setKeyPem] = useState('');
  const [fieldError, setFieldError] = useState<string | null>(null);

  const mutation = useMutation({
    mutationFn: (request: { cert_pem: string; key_pem: string }) => uploadWorkerApiCert(worker, request),
    onSuccess: async () => {
      setCertPem('');
      setKeyPem('');
      setFieldError(null);
      onClose();
      // Статусы применения пересчитает следующий тик — инвалидация ускоряет.
      await queryClient.invalidateQueries({ queryKey: workerQueryKeys.workers });
    },
  });

  const serverError = mutation.error instanceof ApiError ? mutation.error : null;

  // Зеркало §4.3 (клиентская часть): непустые поля с корректными PEM-заголовками.
  function validate(): string | null {
    if (!certPem.includes('-----BEGIN CERTIFICATE-----')) return 'certificate: PEM -----BEGIN CERTIFICATE----- expected';
    if (!keyPem.includes('-----BEGIN PRIVATE KEY-----')) return 'key: PKCS#8 PEM -----BEGIN PRIVATE KEY----- expected';
    return null;
  }

  function submit() {
    const error = validate();
    setFieldError(error);
    if (error !== null) return;
    mutation.mutate({ cert_pem: certPem, key_pem: keyPem });
  }

  async function pickFile(setter: (text: string) => void, file: File | null) {
    if (file !== null) setter(await file.text());
  }

  return (
    <Modal opened={opened} onClose={onClose} title={`API cert of ${worker}: upload a PEM pair`} size="lg">
      <Stack gap="sm">
        {serverError !== null && (
          <Alert color="red" title={serverError.title ?? `HTTP ${serverError.status}`}>
            {serverError.detail ?? serverError.message}
          </Alert>
        )}
        <Group justify="space-between">
          <span>Certificate (PEM)</span>
          <FileButton onChange={(f) => void pickFile(setCertPem, f)} accept=".pem,.crt,.cer,text/plain">
            {(props) => <Button variant="light" size="xs" {...props}>From file…</Button>}
          </FileButton>
        </Group>
        <Textarea
          value={certPem}
          onChange={(e) => setCertPem(e.currentTarget.value)}
          placeholder="-----BEGIN CERTIFICATE-----"
          autosize
          minRows={5}
          data-autofocus
        />
        <Group justify="space-between">
          <span>Private key (PKCS#8 PEM)</span>
          <FileButton onChange={(f) => void pickFile(setKeyPem, f)} accept=".pem,.key,text/plain">
            {(props) => <Button variant="light" size="xs" {...props}>From file…</Button>}
          </FileButton>
        </Group>
        <Textarea
          value={keyPem}
          onChange={(e) => setKeyPem(e.currentTarget.value)}
          placeholder="-----BEGIN PRIVATE KEY-----"
          autosize
          minRows={5}
        />
        {fieldError !== null && <Alert color="yellow">{fieldError}</Alert>}
        <Group justify="flex-end">
          <Button variant="default" onClick={onClose}>Cancel</Button>
          <Button onClick={submit} loading={mutation.isPending}>Upload</Button>
        </Group>
      </Stack>
    </Modal>
  );
}
