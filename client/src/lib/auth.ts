import { useState, useEffect } from 'react';

export interface AuthUser {
  id: number;
  username: string;
  email: string;
  role: "superuser" | "admin" | "vendor";
  clientId: number;
  permissions?: string[];
  mustChangePassword?: boolean;
}

export interface AuthClient {
  id: number;
  name: string;
}

export interface AuthState {
  user: AuthUser | null;
  client: AuthClient | null;
  isAuthenticated: boolean;
}

export const getStoredAuth = (): AuthState => {
  try {
    const stored = localStorage.getItem("auth");
    if (stored) {
      const parsed = JSON.parse(stored);
      return {
        user: parsed.user,
        client: parsed.client,
        isAuthenticated: !!parsed.user,
      };
    }
  } catch (error) {
    console.error("Error parsing stored auth:", error);
  }
  
  return {
    user: null,
    client: null,
    isAuthenticated: false,
  };
};

export const setStoredAuth = (user: AuthUser, client: AuthClient) => {
  const authState = { user, client };
  localStorage.setItem("auth", JSON.stringify(authState));
};

export const clearStoredAuth = () => {
  localStorage.removeItem("auth");
};

// Hook para usar en componentes
export const useAuth = () => {
  const [authState, setAuthState] = useState<AuthState>(getStoredAuth());

  useEffect(() => {
    const handleStorageChange = () => {
      setAuthState(getStoredAuth());
    };

    window.addEventListener('storage', handleStorageChange);
    return () => window.removeEventListener('storage', handleStorageChange);
  }, []);

  const login = (user: AuthUser, client: AuthClient) => {
    setStoredAuth(user, client);
    setAuthState({ user, client, isAuthenticated: true });
  };

  const logout = () => {
    clearStoredAuth();
    setAuthState({ user: null, client: null, isAuthenticated: false });
  };

  return {
    ...authState,
    login,
    logout,
  };
};