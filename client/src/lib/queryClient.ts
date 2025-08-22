import { QueryClient, QueryFunction } from "@tanstack/react-query";
import { getStoredAuth } from "@/lib/auth";

async function throwIfResNotOk(res: Response) {
  if (!res.ok) {
    const text = (await res.text()) || res.statusText;
    throw new Error(`${res.status}: ${text}`);
  }
}

export async function apiRequest(
  method: string,
  url: string,
  data?: unknown | undefined,
): Promise<Response> {
  const auth = getStoredAuth();
  const headers: HeadersInit = {};
  
  if (data) {
    headers["Content-Type"] = "application/json";
  }
  
  // Add user authentication header if available
  if (auth.user?.id) {
    headers['x-user-id'] = auth.user.id.toString();
  }

  const res = await fetch(url, {
    method,
    headers,
    body: data ? JSON.stringify(data) : undefined,
    credentials: "include",
  });

  await throwIfResNotOk(res);
  return res;
}

type UnauthorizedBehavior = "returnNull" | "throw";
export const getQueryFn: <T>(options: {
  on401: UnauthorizedBehavior;
}) => QueryFunction<T> =
  ({ on401: unauthorizedBehavior }) =>
  async ({ queryKey }) => {
    const auth = getStoredAuth();
    const headers: HeadersInit = {
      'Content-Type': 'application/json',
    };
    
    // Add user authentication header if available
    if (auth.user?.id) {
      headers['x-user-id'] = auth.user.id.toString();
    }

    const res = await fetch(queryKey.join("/") as string, {
      credentials: "include",
      headers,
    });

    if (unauthorizedBehavior === "returnNull" && res.status === 401) {
      return null;
    }

    await throwIfResNotOk(res);
    return await res.json();
  };

export const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      queryFn: getQueryFn({ on401: "throw" }),
      refetchInterval: false, // Only refresh manually to avoid constant updates
      refetchOnWindowFocus: true,
      staleTime: 60000, // Consider data stale after 1 minute
      retry: false,
    },
    mutations: {
      retry: false,
    },
  },
});
