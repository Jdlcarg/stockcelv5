import { create } from "zustand";
import { AuthUser, AuthClient, getStoredAuth, setStoredAuth, clearStoredAuth } from "@/lib/auth";

interface AuthStore {
  user: AuthUser | null;
  client: AuthClient | null;
  isAuthenticated: boolean;
  login: (user: AuthUser, client: AuthClient) => void;
  logout: () => void;
}

export const useAuthStore = create<AuthStore>((set) => ({
  ...getStoredAuth(),
  
  login: (user: AuthUser, client: AuthClient) => {
    setStoredAuth(user, client);
    set({ user, client, isAuthenticated: true });
  },
  
  logout: () => {
    clearStoredAuth();
    set({ user: null, client: null, isAuthenticated: false });
  },
}));