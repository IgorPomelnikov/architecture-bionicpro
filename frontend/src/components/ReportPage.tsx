import React, { useCallback, useEffect, useState } from 'react';

const apiBase = process.env.REACT_APP_API_URL ?? '';
const bffBase = apiBase;

type BffUserClaim = {
  type: string;
  value: string;
};

const ReportPage: React.FC = () => {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [userName, setUserName] = useState<string | null>(null);
  const [checking, setChecking] = useState(true);
  const getLoginUrl = useCallback(() => {
    const returnUrl = `${window.location.origin}/`;
    return `${bffBase}/bff/login?returnUrl=${encodeURIComponent(returnUrl)}`;
  }, []);

  const checkSession = useCallback(async () => {
    if (!bffBase) {
      setError('REACT_APP_API_URL is not set (BFF base URL)');
      setChecking(false);
      return;
    }
    try {
      const response = await fetch(`${bffBase}/bff/user`, {
        credentials: 'include',
      });
      if (response.status === 401) {
        window.location.href = getLoginUrl();
        return;
      }
      if (response.ok) {
        const claims = (await response.json()) as BffUserClaim[];
        const userClaim =
          claims.find((claim) => claim.type === 'preferred_username') ??
          claims.find((claim) => claim.type === 'name') ??
          claims.find((claim) => claim.type === 'given_name');
        setUserName(userClaim?.value ?? null);
      } else {
        setUserName(null);
      }
    } catch {
      setUserName(null);
    } finally {
      setChecking(false);
    }
  }, [getLoginUrl]);

  useEffect(() => {
    void checkSession();
  }, [checkSession]);

  const login = () => {
    window.location.href = getLoginUrl();
  };

  const logout = () => {
    const returnUrl = `${window.location.origin}/`;
    window.location.href = `${bffBase}/bff/logout?returnUrl=${encodeURIComponent(returnUrl)}`;
  };

  const downloadReport = async () => {
    if (!bffBase) {
      setError('REACT_APP_API_URL is not set (BFF base URL)');
      return;
    }

    try {
      setLoading(true);
      setError(null);

      const response = await fetch(`${bffBase}/api/reports`, {
        credentials: 'include',
      });
      if (response.status === 401) {
        window.location.href = getLoginUrl();
        return;
      }

      if (!response.ok) {
        setError(`Request failed: ${response.status}`);
        return;
      }

      await response.json();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'An error occurred');
    } finally {
      setLoading(false);
    }
  };

  if (checking) {
    return <div>Loading...</div>;
  }

  if (!userName) {
    return (
      <div className="flex flex-col items-center justify-center min-h-screen bg-gray-100">
        <button
          type="button"
          onClick={login}
          className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600"
        >
          Login (BFF + OIDC code PKCE)
        </button>
        {error && (
          <div className="mt-4 p-4 bg-red-100 text-red-700 rounded">{error}</div>
        )}
      </div>
    );
  }

  return (
    <div className="flex flex-col items-center justify-center min-h-screen bg-gray-100">
      <div className="p-8 bg-white rounded-lg shadow-md">
        <p className="text-sm text-gray-600 mb-2">Вы вошли как {userName}</p>
        <h1 className="text-2xl font-bold mb-6">Usage Reports</h1>

        <div className="flex gap-2 mb-4">
          <button
            type="button"
            onClick={downloadReport}
            disabled={loading}
            className={`px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600 ${
              loading ? 'opacity-50 cursor-not-allowed' : ''
            }`}
          >
            {loading ? 'Generating Report...' : 'Download Report'}
          </button>
          <button
            type="button"
            onClick={logout}
            className="px-4 py-2 bg-gray-200 rounded hover:bg-gray-300"
          >
            Logout
          </button>
        </div>

        {error && (
          <div className="mt-4 p-4 bg-red-100 text-red-700 rounded">{error}</div>
        )}
      </div>
    </div>
  );
};

export default ReportPage;
