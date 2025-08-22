import { createContext, useContext, useState, useEffect, ReactNode } from "react";
import { AuthUser, AuthClient, AuthState, getStoredAuth, setStoredAuth, clearStoredAuth } from "@/lib/auth";
import { apiRequest } from "@/lib/queryClient";

interface AuthContextType extends AuthState {
  login: (email: string, password: string) => Promise<void>;
  logout: () => Promise<void>;
  updateUser: (updatedUser: AuthUser) => void;
  updateClient: (updatedClient: AuthClient) => void;
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [authState, setAuthState] = useState<AuthState>(() => getStoredAuth());

  useEffect(() => {
    const stored = getStoredAuth();
    if (stored.isAuthenticated) {
      setAuthState(stored);
    }
  }, []);

  const login = async (email: string, password: string) => {
    try {
      const response = await apiRequest("POST", "/api/auth/login", { email, password });
      const data = await response.json();
      
      setStoredAuth(data.user, data.client);
      setAuthState({
        user: data.user,
        client: data.client,
        isAuthenticated: true,
      });
    } catch (error) {
      throw new Error("Invalid credentials");
    }
  };

  const logout = async () => {
    try {
      await apiRequest("POST", "/api/auth/logout");
    } catch (error) {
      // Ignore logout errors
    } finally {
      clearStoredAuth();
      setAuthState({
        user: null,
        client: null,
        isAuthenticated: false,
      });
    }
  };

  const updateUser = (updatedUser: AuthUser) => {
    if (authState.client) {
      setStoredAuth(updatedUser, authState.client);
      setAuthState(prev => ({
        ...prev,
        user: updatedUser,
      }));
    }
  };

  const updateClient = (updatedClient: AuthClient) => {
    if (authState.user) {
      setStoredAuth(authState.user, updatedClient);
      setAuthState(prev => ({
        ...prev,
        client: updatedClient,
      }));
    }
  };

  return (
    <AuthContext.Provider value={{ ...authState, login, logout, updateUser, updateClient }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (context === undefined) {
    throw new Error("useAuth must be used within an AuthProvider");
  }
  return context;
}
