/*
*   `readonly` means: Once the object has created, Typescript will not allow you to assign a new value 
*   to that property
*   Note: `readonly` is primarily TS compile time restriction, it does not freeze the JavaScript object at runtime
*/
export type Result<T, E> =
    | { readonly ok: true; readonly value: T }
    | { readonly ok: false; readonly error: E };

/*
*   never in TS means: "This value can never exist." It represent an impossible value/ state.
*/
export function Ok<T>(value: T): Result<T, never> {
    return { ok: true, value };
}

export function Err<E>(error: E): Result<never, E> {
    return { ok: false, error };
}

export function unwrap<T, E>(result: Result<T, E>): T {
    if (result.ok) {
        return result.value;
    }

    throw result.error;
}

/*
*   `result is { ok: true; value: T}` : This part is a TS type predicate. It tells TS:
*   "If the function returns true, you can treat result as { ok: true; value: T}"
* 
*   So this function is basically a type-aware boolean check
* 
*   Imagine you make a check function like this:
*   function isOk<T, E>(result: Result<T, E>): boolean {
*       return result.ok;
*   }
*   
*   if(isOk(result)){
*       console.log(result.value);
*   }
* 
*   TS only knows: isOk(result) === true
*   It does not automatically know that result must be the success variant
*   So result.value may still give a type error. Thats why we write:
*   result is { ok: true; value: T }
* 
*   This pattern is called a user-defined type guard (or type predicate )
* 
*   Without a helper function:
*   if(result.ok){
*       result.value;
*   }
* 
*   TS sees the whole union as the type of result: Result (defined at the top)
*   It knows:
*   result.ok === true -> there is only one union member with ok: true -> therefore result.value exists
*   So typescript narrows it.
*   
*   Now putting the check inside a function:
*   function isOk<T, E>(result: Result<T, E>): boolean {
*       return result.ok;
*   }
* 
*   Then:
*   if(isOk(result)){
*       result.value; Typescript doesn't know this is safe
*   }
* 
*   Why ?
*   Because all TS knows about isOk() is: "This function returns a boolean"
*   It doesn't know what that boolean means
* 
*   So you explicitly tell TypeScript:
*   result is { ok: true; value: T }
*   which means "This boolean isnt just any boolean. When it's true, it guarantees that result is the success variant"
*   
*   Then:
*   if(isOk(result)) {
*       result.value;
*   }
*   TS can safely narrow it.
* 
*   The `is` part is not checking anything at runtime, the information is for TS compiler
*/
export function isOk<T, E>(result: Result<T, E>): result is { ok: true; value: T } {
    return result.ok;
}

export function isErr<T, E>(result: Result<T, E>): result is { ok: false; error: E } {
    return !result.ok;
}